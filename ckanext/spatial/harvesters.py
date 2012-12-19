#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Different harvesters for spatial metadata

These are designed for harvesting GEMINI2 for the UK Location Programme
but can be easily adapted for other INSPIRE/ISO19139 XML metadata
    - GeminiCswHarvester - CSW servers
    - GeminiDocHarvester - An individual GEMINI resource
    - GeminiWafHarvester - An index page with links to GEMINI resources

TODO: Harvesters for generic INSPIRE CSW servers

'''
import cgitb
import warnings
import urllib2
from urlparse import urlparse
from datetime import datetime
from string import Template
from numbers import Number
import sys
import re
import uuid
import os
import tempfile
import logging

from lxml import etree
from pylons import config
from sqlalchemy.sql import update,and_, bindparam
from sqlalchemy.exc import InvalidRequestError
from owslib.csw import namespaces
from owslib import wms

from ckan import model
from ckan.model import Session, repo, \
                        Package, Resource, PackageExtra, \
                        setup_default_user_roles
from ckan.lib.munge import munge_title_to_name
from ckan.plugins.core import SingletonPlugin, implements
from ckan.lib.helpers import json

from ckan import logic
from ckan.logic import get_action, ValidationError
from ckan.lib.navl.validators import not_empty, ignore_missing

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from ckanext.spatial.model import GeminiDocument, InspireDocument
from ckanext.spatial.lib.csw_client import CswService
from ckanext.spatial.lib.groupmap import Util
from ckanext.spatial.validation import Validators, all_validators
from ckanext.spatial.lib.durationmap import DurationTranslator

import ckanext.spatial.lib.license_map
from urllib2 import HTTPError
from urlparse import urlparse

log = logging.getLogger(__name__)

DEFAULT_VALIDATOR_PROFILES = ['iso19139']


def text_traceback():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = 'the original traceback:'.join(
            cgitb.text(sys.exc_info()).split('the original traceback:')[1:]
        ).strip()
    return res

# When developing, it might be helpful to 'export DEBUG=1' to reraise the
# exceptions, rather them being caught.
debug_exception_mode = bool(os.getenv('DEBUG'))

class SpatialHarvester(object):
    # Q: Why does this not inherit from HarvesterBase in ckanext-harvest?

    def _is_wms(self,url):
        try:
            capabilities_url = wms.WMSCapabilitiesReader().capabilities_url(url)
            res = urllib2.urlopen(capabilities_url,None,10)
            xml = res.read()

            s = wms.WebMapService(url,xml=xml)
            return isinstance(s.contents, dict) and s.contents != {}
        except Exception, e:
            log.error('WMS check for %s failed with exception: %s' % (url, str(e)))
            return False

    def _get_validator(self):
        '''
        Returns the validator object using the relevant profiles

        The profiles to be used are assigned in the following order:

        1. 'validator_profiles' property of the harvest source config object
        2. 'ckan.spatial.validator.profiles' configuration option in the ini file
        3. Default value as defined in DEFAULT_VALIDATOR_PROFILES
        '''
        if not hasattr(self, '_validator'):
            if hasattr(self, 'config') and self.config.get('validator_profiles',None):
                profiles = self.config.get('validator_profiles')
            elif config.get('ckan.spatial.validator.profiles', None):
                profiles = [
                    x.strip() for x in
                    config.get('ckan.spatial.validator.profiles').split(',')
                ]
            else:
                profiles = DEFAULT_VALIDATOR_PROFILES
            self._validator = Validators(profiles=profiles)
        return self._validator

    def _save_gather_error(self,message,job):
        err = HarvestGatherError(message=message,job=job)
        try:
            err.save()
        except InvalidRequestError:
            Session.rollback()
            err.save()
        finally:
            log.error(message)

    def _save_object_error(self,message,obj,stage=u'Fetch'):
        err = HarvestObjectError(message=message,object=obj,stage=stage)
        try:
            err.save()
        except InvalidRequestError,e:
            Session.rollback()
            err.save()
        finally:
            log.error(message)

    def _get_content(self, url):
        url = url.replace(' ','%20')
        http_response = urllib2.urlopen(url)
        return http_response.read()

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def validate_config(self,config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'validator_profiles' in config_obj:
                if not isinstance(config_obj['validator_profiles'],list):
                    raise ValueError('validator_profiles must be a list')

                # Check if all profiles exist
                existing_profiles = [v.name for v in all_validators]
                unknown_profiles = set(config_obj['validator_profiles']) - set(existing_profiles)

                if len(unknown_profiles) > 0:
                    raise ValueError('Unknown validation profile(s): %s' % ','.join(unknown_profiles))

        except ValueError,e:
            raise e

        return config

class GeminiHarvester(SpatialHarvester):
    '''Base class for spatial harvesting GEMINI2 documents for the UK Location
    Programme. May be easily adaptable for other INSPIRE and spatial projects.

    All three harvesters share the same import stage
    '''

    force_import = False

    extent_template = Template('''
    {"type":"Polygon","coordinates":[[[$minx, $miny],[$minx, $maxy], [$maxx, $maxy], [$maxx, $miny], [$minx, $miny]]]}
    ''')

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object: %r', harvest_object)

        if not harvest_object:
            log.error('No harvest object received')
            return False

        # Save a reference
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False
        try:
            self.import_gemini_object(harvest_object.content)
            return True
        except Exception, e:
            log.error('Exception during import: %s' % text_traceback())
            if not str(e).strip():
                self._save_object_error('Error importing Gemini document.', harvest_object, 'Import')
            else:
                self._save_object_error('Error importing Gemini document: %s' % str(e), harvest_object, 'Import')

            if debug_exception_mode:
                raise

    def import_gemini_object(self, gemini_string):
        log = logging.getLogger(__name__ + '.import')
        xml = etree.fromstring(gemini_string)

        valid, messages = self._get_validator().is_valid(xml)
        if not valid:
            log.error('Errors found for object with GUID %s:' % self.obj.guid)
            out = messages[0] + ':\n' + '\n'.join(messages[1:])
            self._save_object_error(out,self.obj,'Import')

        unicode_gemini_string = etree.tostring(xml, encoding=unicode, pretty_print=True)

        package = self.write_package_from_gemini_string(unicode_gemini_string)

    def import_inspire_object(self, gemini_string,harvest_object):
        log = logging.getLogger(__name__ + '.import')
        xml = etree.fromstring(gemini_string)

        #valid, messages = self._get_validator().is_valid(xml)
        #if not valid:
        #    log.error('Errors found for object with GUID %s:' % self.obj.guid)
        #    out = messages[0] + ':\n' + '\n'.join(messages[1:])
        #    self._save_object_error(out,self.obj,'Import')

        unicode_inspire_string = etree.tostring(xml, encoding=unicode, pretty_print=True)

        package = self.write_package_from_inspire_string(unicode_inspire_string,harvest_object)

    def write_package_from_gemini_string(self, content):
        '''Create or update a Package based on some content that has
        come from a URL.
        '''
        log = logging.getLogger(__name__ + '.import')
        package = None
        gemini_document = GeminiDocument(content)
        gemini_values = gemini_document.read_values()
        gemini_guid = gemini_values['guid']

        # Save the metadata reference date in the Harvest Object
        try:
            metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%d')
        except ValueError:
            try:
                metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%dT%H:%M:%S')
            except:
                raise Exception('Could not extract reference date for GUID %s (%s)' \
                        % (gemini_guid,gemini_values['metadata-date']))

        self.obj.metadata_modified_date = metadata_modified_date
        self.obj.save()

        last_harvested_object = Session.query(HarvestObject) \
                            .filter(HarvestObject.guid==gemini_guid) \
                            .filter(HarvestObject.current==True) \
                            .all()

        if len(last_harvested_object) == 1:
            last_harvested_object = last_harvested_object[0]
        elif len(last_harvested_object) > 1:
                raise Exception('Application Error: more than one current record for GUID %s' % gemini_guid)

        reactivate_package = False
        if last_harvested_object:
            # We've previously harvested this (i.e. it's an update)

            # Use metadata modified date instead of content to determine if the package
            # needs to be updated
            if last_harvested_object.metadata_modified_date is None \
                or last_harvested_object.metadata_modified_date < self.obj.metadata_modified_date \
                or self.force_import \
                or (last_harvested_object.metadata_modified_date == self.obj.metadata_modified_date and
                    last_harvested_object.source.active is False):

                if self.force_import:
                    log.info('Import forced for object %s with GUID %s' % (self.obj.id,gemini_guid))
                else:
                    log.info('Package for object with GUID %s needs to be created or updated' % gemini_guid)

                package = last_harvested_object.package

                # If the package has a deleted state, we will only update it and reactivate it if the
                # new document has a more recent modified date
                if package.state == u'deleted':
                    if last_harvested_object.metadata_modified_date < self.obj.metadata_modified_date:
                        log.info('Package for object with GUID %s will be re-activated' % gemini_guid)
                        reactivate_package = True
                    else:
                         log.info('Remote record with GUID %s is not more recent than a deleted package, skipping... ' % gemini_guid)
                         return None

            else:
                if last_harvested_object.content != self.obj.content and \
                 last_harvested_object.metadata_modified_date == self.obj.metadata_modified_date:
                    raise Exception('The contents of document with GUID %s changed, but the metadata date has not been updated' % gemini_guid)
                else:
                    # The content hasn't changed, no need to update the package
                    log.info('Document with GUID %s unchanged, skipping...' % (gemini_guid))
                return None
        else:
            log.info('No package with GEMINI guid %s found, let''s create one' % gemini_guid)

        extras = {
            'published_by': self.obj.source.publisher_id or '',
            'UKLP': 'True',
            'harvest_object_id': self.obj.id
        }

        # Just add some of the metadata as extras, not the whole lot
        for name in [
            # Essentials
            'bbox-east-long',
            'bbox-north-lat',
            'bbox-south-lat',
            'bbox-west-long',
            'spatial-reference-system',
            'guid',
            # Usefuls
            'dataset-reference-date',
            'resource-type',
            'metadata-language', # Language
            'metadata-date', # Released
            'coupled-resource',
            'contact-email',
            'frequency-of-update',
            'spatial-data-service-type',
        ]:
            extras[name] = gemini_values[name]

        extras['licence'] = gemini_values.get('use-constraints', '')
        if len(extras['licence']):
            license_url_extracted = self._extract_first_license_url(extras['licence'])
            if license_url_extracted:
                extras['licence_url'] = license_url_extracted

        extras['access_constraints'] = gemini_values.get('limitations-on-public-access','')
        if gemini_values.has_key('temporal-extent-begin'):
            #gemini_values['temporal-extent-begin'].sort()
            extras['temporal_coverage-from'] = gemini_values['temporal-extent-begin']
        if gemini_values.has_key('temporal-extent-end'):
            #gemini_values['temporal-extent-end'].sort()
            extras['temporal_coverage-to'] = gemini_values['temporal-extent-end']



        # Save responsible organization roles
        parties = {}
        owners = []
        publishers = []
        for responsible_party in gemini_values['responsible-organisation']:

            if responsible_party['role'] == 'owner':
                owners.append(responsible_party['organisation-name'])
            elif responsible_party['role'] == 'publisher':
                publishers.append(responsible_party['organisation-name'])

            if responsible_party['organisation-name'] in parties:
                if not responsible_party['role'] in parties[responsible_party['organisation-name']]:
                    parties[responsible_party['organisation-name']].append(responsible_party['role'])
            else:
                parties[responsible_party['organisation-name']] = [responsible_party['role']]

        parties_extra = []
        for party_name in parties:
            parties_extra.append('%s (%s)' % (party_name, ', '.join(parties[party_name])))
        extras['responsible-party'] = '; '.join(parties_extra)

        # Save provider in a separate extra:
        # first organization to have a role of 'owner', and if there is none, first one with
        # a role of 'publisher'
        if len(owners):
            extras['provider'] = owners[0]
        elif len(publishers):
            extras['provider'] = publishers[0]
        else:
            extras['provider'] = u''

        # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry
        extent_string = self.extent_template.substitute(
                minx = extras['bbox-east-long'],
                miny = extras['bbox-south-lat'],
                maxx = extras['bbox-west-long'],
                maxy = extras['bbox-north-lat']
                )

        extras['spatial'] = extent_string.strip()

        tags = []
        for tag in gemini_values['tags']:
            tag = tag[:50] if len(tag) > 50 else tag
            tags.append({'name':tag})

        package_dict = {
            'title': gemini_values['title'],
            'notes': gemini_values['abstract'],
            'tags': tags,
            'resources':[]
        }

        if self.obj.source.publisher_id:
            package_dict['groups'] = [{'id':self.obj.source.publisher_id}]


        if reactivate_package:
            package_dict['state'] = u'active'

        if package is None or package.title != gemini_values['title']:
            name = self.gen_new_name(gemini_values['title'])
            if not name:
                name = self.gen_new_name(str(gemini_guid))
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
            package_dict['name'] = name
        else:
            package_dict['name'] = package.name

        resource_locators = gemini_values.get('resource-locator', [])

        if len(resource_locators):
            for resource_locator in resource_locators:
                url = resource_locator.get('url','')
                if url:
                    resource_format = ''
                    resource = {}
                    if extras['resource-type'] == 'service':
                        # Check if the service is a view service
                        test_url = url.split('?')[0] if '?' in url else url
                        if self._is_wms(test_url):
                            resource['verified'] = True
                            resource['verified_date'] = datetime.now().isoformat()
                            resource_format = 'WMS'
                    resource.update(
                        {
                            'url': url,
                            'name': resource_locator.get('name',''),
                            'description': resource_locator.get('description') if resource_locator.get('description') else 'Resource locator',
                            'format': resource_format or None,
                            'resource_locator_protocol': resource_locator.get('protocol',''),
                            'resource_locator_function':resource_locator.get('function','')

                        })
                    package_dict['resources'].append(resource)

            # Guess the best view service to use in WMS preview
            verified_view_resources = [r for r in package_dict['resources'] if 'verified' in r and r['format'] == 'WMS']
            if len(verified_view_resources):
                verified_view_resources[0]['ckan_recommended_wms_preview'] = True
            else:
                view_resources = [r for r in package_dict['resources'] if r['format'] == 'WMS']
                if len(view_resources):
                    view_resources[0]['ckan_recommended_wms_preview'] = True

        extras_as_dict = []
        for key,value in extras.iteritems():
            if isinstance(value,(basestring,Number)):
                extras_as_dict.append({'key':key,'value':value})
            else:
                extras_as_dict.append({'key':key,'value':json.dumps(value)})

        package_dict['extras'] = extras_as_dict

        if not package_dict['resources']:
            log.error('Package with GUID %s does not contain any resources, skip this package' % self.obj.guid)
            out = "Package does not contain any resources"
            self._save_object_error(out,self.obj,'Import')
            #log.info("Package does not contain any resources, skip this package!")
            return None
        else:
            if package == None:
                # Create new package from data.
                package = self._create_package_from_data(package_dict)
                log.info('Created new package ID %s with GEMINI guid %s', package['id'], gemini_guid)
            else:
                package = self._create_package_from_data(package_dict, package = package)
                log.info('Updated existing package ID %s with existing GEMINI guid %s', package['id'], gemini_guid)
    
            # Flag the other objects of this source as not current anymore
            from ckanext.harvest.model import harvest_object_table
            u = update(harvest_object_table) \
                    .where(harvest_object_table.c.package_id==bindparam('b_package_id')) \
                    .values(current=False)
            Session.execute(u, params={'b_package_id':package['id']})
            Session.commit()
    
            # Refresh current object from session, otherwise the
            # import paster command fails
            Session.remove()
            Session.add(self.obj)
            Session.refresh(self.obj)
    
            # Set reference to package in the HarvestObject and flag it as
            # the current one
            if not self.obj.package_id:
                self.obj.package_id = package['id']
    
            self.obj.current = True
            self.obj.save()
    
    
            assert gemini_guid == [e['value'] for e in package['extras'] if e['key'] == 'guid'][0]
            assert self.obj.id == [e['value'] for e in package['extras'] if e['key'] == 'harvest_object_id'][0]
    
            return package


    def _is_pdf_URI(self, url):
        if url.endswith(".pdf"):
            return True
        else:
            req = urllib2.urlopen(url)
            headers = req.headers['content-type']
            #TODO application/octet-stream isbinary, accept as pdf?
            return 'application/pdf' in headers
    
    def _is_htm_or_html(self, url):
        if url.endswith('.htm') or url.endswith('html'):
            log.info("%s is html page" %url)
            return True
        else:
            log.info('try to open url: %s' %url)
            req = urllib2.urlopen(url)
            headers = req.headers['content-type']
            if 'text/html' in headers:
                log.info("%s is html page" %url)
                return True
    
    
    def _is_probably_wms(self, url):
        log.info('try to open url: %s' %url)
        try:
            urllib2.urlopen(url)
        except HTTPError :
            return False
        else:
            # status code is 200
            # accept this URL as WMS
            return True
    
    def handle_resources(self,resource_locators):
        ''' Handle all resources except for WMS endpoints '''
        result = []
        if len(resource_locators):
            log.info("Found %s resources" %len(resource_locators))
            for resource_locator in resource_locators:
                url = resource_locator.get('url','')
                if url:
                    resource_format = ''
                    resource = {}
                    if self._is_htm_or_html(url):
                        continue
                    if self._is_pdf_URI(url):
                        resource_format = 'PDF'
                    elif self._is_wms(url):
                        resource['verified'] = True
                        resource['verified_date'] = datetime.now().isoformat()
                        resource_format = 'WMS'
                    resource.update(
                        {
                            'url': url,
                            'name': resource_locator.get('name',''),
                            'description': resource_locator.get('description') if resource_locator.get('description') else (resource_format + ' - Ressource'),
                            'format': resource_format or None,
                            'resource_locator_protocol': resource_locator.get('protocol',''),
                            'resource_locator_function':resource_locator.get('function','')

                        })
                    result.append(resource)
        return result
    
    def handle_services(self,service_locators):
        ''' Handle WMS endpoints '''
        result = []
        if len(service_locators):
            log.info("Found %s service points" % len(service_locators))
            for service_locator in service_locators:
                url = service_locator.get('url', '')
                if url:
                    service_format = ''
                    resource = {}
                    if self._is_wms(url):
                        resource['verified'] = True
                        resource['verified_date'] = datetime.now().isoformat()
                        service_format = 'WMS'
                    elif self._is_probably_wms(url):
                        # check if wms is alive
                        service_format = 'WMS'
                    else:
                        log.info('Invalid WMS Service!')
                    resource.update(
                        {
                            'url': url,
                            'name': service_locator.get('name',''),
                            'description': service_locator.get('description') if service_locator.get('description') else 'Ressource',
                            'format': service_format or None,
                            'resource_locator_protocol': service_locator.get('protocol',''),
                            'resource_locator_function':service_locator.get('function','')

                        })
                    result.append(resource)
        return result

    def handle_licenses(self,gemini_values):
        return ckanext.spatial.lib.license_map.translate_license_data(gemini_values)

    def write_package_from_inspire_string(self, content, harvest_object):
        '''Create or update a package based on fetched INSPIRE content'''

        log = logging.getLogger(__name__ + '.import')
        package = None
        gemini_document = InspireDocument(content)
        gemini_values = gemini_document.read_values()
        gemini_guid = gemini_values['guid']

        # Save the metadata reference date in the Harvest Object
        try:
            metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%d')
        except ValueError:
            try:
                metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%dT%H:%M:%S')
            except:
                raise Exception('Could not extract reference date for GUID %s (%s)' \
                        % (gemini_guid,gemini_values['metadata-date']))

        self.obj.metadata_modified_date = metadata_modified_date
        self.obj.save()

        last_harvested_object = Session.query(HarvestObject) \
                            .filter(HarvestObject.guid==gemini_guid) \
                            .filter(HarvestObject.current==True) \
                            .all()

        if len(last_harvested_object) == 1:
            last_harvested_object = last_harvested_object[0]
        elif len(last_harvested_object) > 1:
                raise Exception('Application Error: more than one current record for GUID %s' % gemini_guid)

        reactivate_package = False
        if last_harvested_object:
            # We've previously harvested this (i.e. it's an update)

            # Use metadata modified date instead of content to determine if the package
            # needs to be updated
            if last_harvested_object.metadata_modified_date is None \
                or last_harvested_object.metadata_modified_date < self.obj.metadata_modified_date \
                or self.force_import \
                or (last_harvested_object.metadata_modified_date == self.obj.metadata_modified_date and
                    last_harvested_object.source.active is False):

                if self.force_import:
                    log.info('Import forced for object %s with GUID %s' % (self.obj.id,gemini_guid))
                else:
                    log.info('Package for object with GUID %s needs to be created or updated' % gemini_guid)

                package = last_harvested_object.package

                # If the package has a deleted state, we will only update it and reactivate it if the
                # new document has a more recent modified date
                if package.state == u'deleted':
                    if last_harvested_object.metadata_modified_date < self.obj.metadata_modified_date:
                        log.info('Package for object with GUID %s will be re-activated' % gemini_guid)
                        reactivate_package = True
                    else:
                         log.info('Remote record with GUID %s is not more recent than a deleted package, skipping... ' % gemini_guid)
                         return None

            else:
                if last_harvested_object.content != self.obj.content and \
                 last_harvested_object.metadata_modified_date == self.obj.metadata_modified_date:
                    raise Exception('The contents of document with GUID %s changed, but the metadata date has not been updated' % gemini_guid)
                else:
                    # The content hasn't changed, no need to update the package
                    log.info('Document with GUID %s unchanged, skipping...' % (gemini_guid))
                return None
        else:
            log.info('No package with INSPIRE guid %s found, let''s create one' % gemini_guid)

        extras = {
            'published_by': self.obj.source.publisher_id or '',
            'UKLP': 'True',
            'harvest_object_id': self.obj.id
        }

        # Just add some of the metadata as extras, not the whole lot
        for name in [
            # Essentials
            'bbox-east-long',
            'bbox-north-lat',
            'bbox-south-lat',
            'bbox-west-long',
            'spatial-reference-system',
            'guid',
            # Usefuls
            'dataset-reference-date',
            'resource-type',
            'metadata-language', # Language
            'metadata-date', # Released
            'coupled-resource',
            'contact-email',
            'frequency-of-update',
            'spatial-data-service-type',
        ]:
            extras[name] = gemini_values[name]
       
        #map given dates to OGPD date fields
        ogpd_date_created = { 'role' : u'erstellt',  'date' : ''}
        ogpd_date_released = { 'role' : u'veroeffentlicht', 'date' : ''}
        dates = []
        
        if gemini_values['date-released']:
            ogpd_date_released['date'] = gemini_values['date-released']    
        else:
            ogpd_date_released['date'] = gemini_values['metadata-date']  
        
        dates.append(ogpd_date_released)     
                           
        if gemini_values['date-updated']:
            for date in gemini_values['date-updated'] :
                ogpd_date_updated = { 'role' : u'aktualisiert', 'date' : date}
                dates.append(ogpd_date_updated)     
        
        if gemini_values['date-created']:
            ogpd_date_created['date'] = (gemini_values['date-created']) [0]
            dates.append(ogpd_date_created)
             
        extras['dates']= dates
        
        #original metadata information  
        url_schema = urlparse(harvest_object.source.url)
        extras['metadata_original_portal'] = url_schema.netloc
        extras['metadata_original_id'] = gemini_values['guid']
        
        csw_request = '?Service=CSW&Request=GetRecordById&Id='
        extras['metadata_original_xml'] = harvest_object.source.url + csw_request + gemini_values['guid']  
     
 
        extras['subgroups'] = gemini_values['topic-category']
        log.debug('Set subgroups: ' + str(gemini_values['topic-category']))

        if gemini_values.has_key('temporal-extent-begin'):
            #gemini_values['temporal-extent-begin'].sort()
            extras['temporal_coverage-from'] = gemini_values['temporal-extent-begin']
        if gemini_values.has_key('temporal-extent-end'):
            #gemini_values['temporal-extent-end'].sort()
            extras['temporal_coverage-to'] = gemini_values['temporal-extent-end']
            
            
                #temporal granularity information
        duration_translator = DurationTranslator()      
        temp_duration = ''
        temp_factor = ''
        duration = None
        
        if gemini_values.has_key('frequency-of-update'): 
            temp_duration = duration_translator.translate_duration_data(gemini_values['frequency-of-update'])  
            
        if gemini_values.has_key('frequency-of-update-factor'):
            duration = duration_translator.translate_duration_factor(gemini_values['frequency-of-update-factor'])
       
        if temp_duration:
            extras['temporal_granularity'] = temp_duration         
            if duration:
                if duration['duration'] == temp_duration:
                    temp_factor =  duration['duration_factor']                
        else:
            if gemini_values['frequency-of-update'] == 'forthnightly':
                    temp_duration = 'Tag'
                    temp_factor = 14   
            else:
                if gemini_values['frequency-of-update'] == 'biannually':
                    temp_duration = 'Monat'
                    temp_factor = 6            
                else:
                    if duration:       
                        temp_duration = duration['duration']
                        temp_factor =  duration['duration_factor']                       
        
        if temp_duration:
            extras['temporal_granularity'] = temp_duration 
        if temp_factor:
            extras['temporal_granularity_factor'] = temp_factor

        # map INSPIRE constraint fields to OGPD license fields
        # terms_of_use = ckanext.spatial.lib.license_map.translate_license_data(gemini_values)
        terms_of_use = self.handle_licenses(gemini_values)

        # terms of use == null indicates to drop the entry completely
        if terms_of_use is None:
                return None

        extras['terms_of_use'] = terms_of_use
        
        # map INSPIRE responsible organisation fields to OGPD contacts
        publisher = { 'role' : u'Veröffentlichende Stelle', 'name' : '', 'url' : '', 'email' : '', 'address' : '' }
        owner = { 'role' : u'Ansprechpartner', 'name' : '', 'url' : '', 'email' : '', 'address' : '' }

        if gemini_values['publisher-email']:
                publisher['email'] = gemini_values['publisher-email']
                
        if gemini_values['owner-email']:
                publisher['email'] = gemini_values['owner-email']

        # Save responsible organization roles
        parties = {}
        owners = []
        publishers = []
        for responsible_party in gemini_values['responsible-organisation']:

            if responsible_party['role'] == 'owner' or responsible_party['role'] == 'pointOfContact':
                owners.append(responsible_party['organisation-name'])
                owner['name'] = responsible_party['organisation-name'] 
            elif responsible_party['role'] == 'publisher':
                publishers.append(responsible_party['organisation-name'])
                publisher['name'] = responsible_party['organisation-name'] 
            if responsible_party['organisation-name'] in parties:
                if not responsible_party['role'] in parties[responsible_party['organisation-name']]:
                    parties[responsible_party['organisation-name']].append(responsible_party['role'])
            else:
                parties[responsible_party['organisation-name']] = [responsible_party['role']]

        extras['contacts'] = [ owner, publisher ]
        
        parties_extra = []
        for party_name in parties:
            parties_extra.append('%s (%s)' % (party_name, ', '.join(parties[party_name])))
        extras['responsible-party'] = '; '.join(parties_extra)

        # Save provider in a separate extra:
        # first organization to have a role of 'owner', and if there is none, first one with
        # a role of 'publisher'
        if len(owners):
            extras['provider'] = owners[0]
        elif len(publishers):
            extras['provider'] = publishers[0]
        else:
            extras['provider'] = u''

        # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry
        extent_string = self.extent_template.substitute(
                minx = extras['bbox-east-long'],
                miny = extras['bbox-south-lat'],
                maxx = extras['bbox-west-long'],
                maxy = extras['bbox-north-lat']
                )

        extras['spatial'] = extent_string.strip()

        # Only [a-zA-Z0-9-_] is allowed, filter every other character
        tags = []
        for tag in gemini_values['tags']:
            tag = tag[:50] if len(tag) > 50 else tag
            tag = re.sub('[^a-zA-Z0-9-_ ]*', '', tag)
            tags.append({'name':tag})
        
        #add groups (mapped from ISO 19115 into OGD schema)      
        u = Util()
        categories = []
        for cat in  u.translate(gemini_values['topic-category'], 'iso'):
            categories.append({'name':cat})
        #add iso groups, it is supposed to be established within the groups (fields groups and type), what is a group and what is a subgroup.
        for cat in  gemini_values['topic-category']:
            categories.append({'name':cat})

        # TODO: the following line is only valid for portalU, pls keep that in mind
        categories.append({'name':'umwelt_klima'})
        
        package_dict = {
            'title': gemini_values['title'],
            'notes': gemini_values['abstract'],
            'tags': tags,
            'groups': categories,
            'resources':[]
        }
        
        #copy license_id to ckan-core license id
        package_dict['license_id'] = extras['terms_of_use']['license_id']
        log.debug('Set license_id to %s' %package_dict['license_id'])
        
        log.debug('Set groups to ' + str(package_dict['groups']))

        if self.obj.source.publisher_id:
            package_dict['groups'] = [{'id':self.obj.source.publisher_id}]


        if reactivate_package:
            package_dict['state'] = u'active'

        if package is None or package.title != gemini_values['title']:
            name = self.gen_new_name(gemini_values['title'])
            if not name:
                name = self.gen_new_name(str(gemini_guid))
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
            package_dict['name'] = name
        else:
            package_dict['name'] = package.name

        resource_locators = gemini_values.get('resource-locator', [])
        resources = self.handle_resources(resource_locators)
        
        package_dict['resources'].extend(resources)

        service_locators = gemini_values.get('service-locator', [])
        services = self.handle_services(service_locators)
        package_dict['resources'].extend(services)
        
        # Guess the best view service to use in WMS preview
        verified_view_resources = [r for r in package_dict['resources'] if 'verified' in r and r['format'] == 'WMS']
        if len(verified_view_resources):
            verified_view_resources[0]['ckan_recommended_wms_preview'] = True
        else:
            view_resources = [r for r in package_dict['resources'] if r['format'] == 'WMS']
            if len(view_resources):
                view_resources[0]['ckan_recommended_wms_preview'] = True

        extras_as_dict = []
        for key,value in extras.iteritems():
            if isinstance(value,(basestring,Number)):
               extras_as_dict.append({'key':key,'value':value})
            else:
               extras_as_dict.append({'key':key,'value':json.dumps(value, ensure_ascii = False)})

        package_dict['extras'] = extras_as_dict

        if not package_dict['resources']:
            log.error('Package with GUID %s does not contain any resources, skip this package' % self.obj.guid)
            out = "Package does not contain any resources"
            self._save_object_error(out,self.obj,'Import')
            return None
        else:
            is_a_document = True
            # check if any resource has a format not equal to 'PDf'
            if [resource for resource in package_dict['resources'] if resource['format'] != 'PDF' ]:
                is_a_document = False
            
            if is_a_document:
                package_dict['type'] = 'dokument'
            else:
                if 'service' in gemini_values['resource-type'] or 'application' in gemini_values['resource-type'] :
                    package_dict['type'] = 'app'
                else:
                    if 'document' in gemini_values['resource-type']:
                        package_dict['type'] = 'dokument'            
                    else:
                        if 'dataset' in gemini_values['resource-type'] or 'nonGeographicDataset' in gemini_values['resource-type'] or 'database' in gemini_values['resource-type'] or  'series' in gemini_values['resource-type']:
                            package_dict['type'] = 'datensatz'
                        else:
                            package_dict['type'] = 'dokument'
                            
                                 
            if package == None:
                # Create new package from data.
                package = self._create_package_from_data(package_dict)
                log.info('Created new package ID %s with GEMINI guid %s', package['id'], gemini_guid)
            else:
                package = self._create_package_from_data(package_dict, package = package)
                log.info('Updated existing package ID %s with existing GEMINI guid %s', package['id'], gemini_guid)
    
            # Flag the other objects of this source as not current anymore
            from ckanext.harvest.model import harvest_object_table
            u = update(harvest_object_table) \
                    .where(harvest_object_table.c.package_id==bindparam('b_package_id')) \
                    .values(current=False)
            Session.execute(u, params={'b_package_id':package['id']})
            Session.commit()
    
            # Refresh current object from session, otherwise the
            # import paster command fails
            Session.remove()
            Session.add(self.obj)
            Session.refresh(self.obj)
    
            # Set reference to package in the HarvestObject and flag it as
            # the current one
            if not self.obj.package_id:
                self.obj.package_id = package['id']
    
            self.obj.current = True
            self.obj.save()
    
    
            assert gemini_guid == [e['value'] for e in package['extras'] if e['key'] == 'guid'][0]
            assert self.obj.id == [e['value'] for e in package['extras'] if e['key'] == 'harvest_object_id'][0]
    
            return package
        
    def gen_new_name(self, title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        like_q = u'%s%%' % name
        pkg_query = Session.query(Package).filter(Package.name.ilike(like_q)).limit(1000000)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 1000001:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None

    def _extract_first_license_url(self,licences):
        for licence in licences:
            o = urlparse(licence)
            if o.scheme and o.netloc:
                return licence
        return None

    def _create_package_from_data(self, package_dict, package = None):
        '''
        {'name': 'council-owned-litter-bins',
         'notes': 'Location of Council owned litter bins within Borough.',
         'resources': [{'description': 'Resource locator',
                        'format': 'Unverified',
                        'url': 'http://www.barrowbc.gov.uk'}],
         'tags': [{'name':'Utility and governmental services'}],
         'title': 'Council Owned Litter Bins',
         'extras': [{'key':'INSPIRE','value':'True'},
                    {'key':'bbox-east-long','value': '-3.12442'},
                    {'key':'bbox-north-lat','value': '54.218407'},
                    {'key':'bbox-south-lat','value': '54.039634'},
                    {'key':'bbox-west-long','value': '-3.32485'},
                    # etc.
                    ]
        }
        '''

        if not package:
            package_schema = logic.schema.default_create_package_schema()
        else:
            package_schema = logic.schema.default_update_package_schema()

        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty,unicode]
        package_schema['tags'] = tag_schema

        # TODO: user
        context = {'model':model,
                   'session':Session,
                   'user':'harvest',
                   'schema':package_schema,
                   'extras_as_string':True,
                   'api_version': '2'}
        if not package:
            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            action_function = get_action('package_create')
        else:
            action_function = get_action('package_update')
            package_dict['id'] = package.id

        try:
            package_dict = action_function(context, package_dict)
        except ValidationError,e:
            raise Exception('Validation Error: %s' % str(e.error_summary))
            if debug_exception_mode:
                raise

        return package_dict

    def get_gemini_string_and_guid(self,content,url=None):
        xml = etree.fromstring(content)

        # The validator and GeminiDocument don't like the container
        metadata_tag = '{http://www.isotc211.org/2005/gmd}MD_Metadata'
        if xml.tag == metadata_tag:
            gemini_xml = xml
        else:
            gemini_xml = xml.find(metadata_tag)

        if gemini_xml is None:
            self._save_gather_error('Content is not a valid Gemini document',self.harvest_job)

        #valid, messages = self._get_validator().is_valid(gemini_xml)
        #if not valid:
        #    out = messages[0] + ':\n' + '\n'.join(messages[1:])
        #    if url:
        #        self._save_gather_error('Validation error for %s - %s'% (url,out),self.harvest_job)
        #    else:
        #        self._save_gather_error('Validation error - %s'%out,self.harvest_job)

        gemini_string = etree.tostring(gemini_xml)
        gemini_document = GeminiDocument(gemini_string)
        gemini_values = gemini_document.read_values()
        gemini_guid = gemini_values['guid']

        return gemini_string, gemini_guid

class GeminiCswHarvester(GeminiHarvester, SingletonPlugin):
    '''
    A Harvester for CSW servers
    '''
    implements(IHarvester)

    csw=None

    def info(self):
        return {
            'name': 'csw',
            'title': 'CSW Server',
            'description': 'A server that implements OGC\'s Catalog Service for the Web (CSW) standard'
            }

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.CSW.gather')
        log.debug('GeminiCswHarvester gather_stage for job: %r', harvest_job)
        # Get source URL
        url = harvest_job.source.url

        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_gather_error('Error contacting the CSW server: %s' % e, harvest_job)
            return None


        log.debug('Starting gathering for %s' % url)
        used_identifiers = []
        ids = []
        try:
            for identifier in self.csw.getidentifiers(page=10):
                try:
                    log.info('Got identifier %s from the CSW', identifier)
                    if identifier in used_identifiers:
                        log.error('CSW identifier %r already used, skipping...' % identifier)
                        continue
                    if identifier is None:
                        log.error('CSW returned identifier %r, skipping...' % identifier)
                        ## log an error here? happens with the dutch data
                        continue

                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid=identifier, job=harvest_job)
                    obj.save()

                    ids.append(obj.id)
                    used_identifiers.append(identifier)
                except Exception, e:
                    self._save_gather_error('Error for the identifier %s [%r]' % (identifier,e), harvest_job)
                    continue

        except Exception, e:
            self._save_gather_error('Error gathering the identifiers from the CSW server [%s]' % str(e), harvest_job)
            return None

        if len(ids) == 0:
            self._save_gather_error('No records received from the CSW server', harvest_job)
            return None

        return ids

    def fetch_stage(self,harvest_object):
        log = logging.getLogger(__name__ + '.CSW.fetch')
        log.debug('GeminiCswHarvester fetch_stage for object: %r', harvest_object)

        url = harvest_object.source.url
        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_object_error('Error contacting the CSW server: %s' % e,
                                    harvest_object)
            return False

        identifier = harvest_object.guid
        try:
            record = self.csw.getrecordbyid([identifier])
        except Exception, e:
            self._save_object_error('Error getting the CSW record with GUID %s' % identifier, harvest_object)
            return False

        if record is None:
            self._save_object_error('Empty record for GUID %s' % identifier,
                                    harvest_object)
            return False

        try:
            # Save the fetch contents in the HarvestObject
            harvest_object.content = record['xml']
            harvest_object.save()
        except Exception,e:
            self._save_object_error('Error saving the harvest object for GUID %s [%r]' % \
                                    (identifier, e), harvest_object)
            return False

        log.debug('XML content saved (len %s)', len(record['xml']))
        return True

    def _setup_csw_client(self, url):
        self.csw = CswService(url)


class GeminiDocHarvester(GeminiHarvester, SingletonPlugin):
    '''
    A Harvester for individual GEMINI documents
    '''

    implements(IHarvester)

    def info(self):
        return {
            'name': 'gemini-single',
            'title': 'Single GEMINI 2 document',
            'description': 'A single GEMINI 2.1 document'
            }

    def gather_stage(self,harvest_job):
        log = logging.getLogger(__name__ + '.individual.gather')
        log.debug('GeminiDocHarvester gather_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source URL
        url = harvest_job.source.url

        # Get contents
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                        (url, e),harvest_job)
            return None
        try:
            # We need to extract the guid to pass it to the next stage
            gemini_string, gemini_guid = self.get_gemini_string_and_guid(content,url)

            if gemini_guid:
                # Create a new HarvestObject for this identifier
                # Generally the content will be set in the fetch stage, but as we alredy
                # have it, we might as well save a request
                obj = HarvestObject(guid=gemini_guid,
                                    job=harvest_job,
                                    content=gemini_string)
                obj.save()

                log.info('Got GUID %s' % gemini_guid)
                return [obj.id]
            else:
                self._save_gather_error('Could not get the GUID for source %s' % url, harvest_job)
                return None
        except Exception, e:
            self._save_gather_error('Error parsing the document. Is this a valid Gemini document?: %s [%r]'% (url,e),harvest_job)
            if debug_exception_mode:
                raise
            return None


    def fetch_stage(self,harvest_object):
        # The fetching was already done in the previous stage
        return True


class GeminiWafHarvester(GeminiHarvester, SingletonPlugin):
    '''
    A Harvester from a WAF server containing GEMINI documents.
    e.g. Apache serving a directory of GEMINI files.
    '''

    implements(IHarvester)

    def info(self):
        return {
            'name': 'gemini-waf',
            'title': 'Web Accessible Folder (WAF) - GEMINI',
            'description': 'A Web Accessible Folder (WAF) displaying a list of GEMINI 2.1 documents'
            }

    def gather_stage(self,harvest_job):
        log = logging.getLogger(__name__ + '.WAF.gather')
        log.debug('GeminiWafHarvester gather_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source URL
        url = harvest_job.source.url

        # Get contents
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                        (url, e),harvest_job)
            return None

        ids = []
        try:
            for url in self._extract_urls(content,url):
                try:
                    content = self._get_content(url)
                except Exception, e:
                    msg = 'Couldn\'t harvest WAF link: %s: %s' % (url, e)
                    self._save_gather_error(msg,harvest_job)
                    continue
                else:
                    # We need to extract the guid to pass it to the next stage
                    try:
                        gemini_string, gemini_guid = self.get_gemini_string_and_guid(content,url)
                        if gemini_guid:
                            log.debug('Got GUID %s' % gemini_guid)
                            # Create a new HarvestObject for this identifier
                            # Generally the content will be set in the fetch stage, but as we alredy
                            # have it, we might as well save a request
                            obj = HarvestObject(guid=gemini_guid,
                                                job=harvest_job,
                                                content=gemini_string)
                            obj.save()

                            ids.append(obj.id)


                    except Exception,e:
                        msg = 'Could not get GUID for source %s: %r' % (url,e)
                        self._save_gather_error(msg,harvest_job)
                        continue
        except Exception,e:
            msg = 'Error extracting URLs from %s' % url
            self._save_gather_error(msg,harvest_job)
            return None


        if len(ids) > 0:
            return ids
        else:
            self._save_gather_error('Couldn''t find any links to metadata files',
                                     harvest_job)
            return None

    def fetch_stage(self,harvest_object):
        # The fetching was already done in the previous stage
        return True


    def _extract_urls(self, content, base_url):
        '''
        Get the URLs out of a WAF index page
        '''
        try:
            parser = etree.HTMLParser()
            tree = etree.fromstring(content, parser=parser)
        except Exception, inst:
            msg = 'Couldn''t parse content into a tree: %s: %s' \
                  % (inst, content)
            raise Exception(msg)
        urls = []
        for url in tree.xpath('//a/@href'):
            url = url.strip()
            if not url:
                continue
            if '?' in url:
                continue
            if '/' in url:
                continue
            if '#' in url:
                continue
            if 'mailto:' in url:
                continue
            urls.append(url)
        base_url = base_url.rstrip('/').split('/')
        if 'index' in base_url[-1]:
            base_url.pop()
        base_url = '/'.join(base_url)
        base_url += '/'
        return [base_url + i for i in urls]

class OGPDHarvester(GeminiCswHarvester, SingletonPlugin):
    '''
    A Harvester for CSW servers, for targeted at import into the German Open Data Platform now focused on Geodatenkatalog-DE
    '''
    implements(IHarvester)

    def info(self):
        return {
            'name': 'ogpd',
            'title': 'OGPD Harvester',
            'description': 'Harvester for CSW Servers like GDI Geodatenkatalog'
            }

    def gather_stage(self,harvest_job):
        log.debug('In OGPDHarvester gather_stage')
        # Get source URL
        url = harvest_job.source.url

        # Setup CSW client
        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_gather_error('Error contacting the CSW server: %s' % e,harvest_job)
            return None


        log.debug('Starting gathering for %s ' % url)
        used_identifiers = []
        ids = []
        try:
            for identifier in self.csw.getidentifiers(limit=1000, page=10):
                try:
                    log.info('Got identifier %s from the CSW', identifier)
                    if identifier in used_identifiers:
                        log.error('CSW identifier %r already used, skipping...' % identifier)
                        continue
                    if identifier is None:
                        log.error('CSW returned identifier %r, skipping...' % identifier)
                        ## log an error here? happens with the dutch data
                        continue

                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = identifier, job = harvest_job)
                    obj.save()

                    ids.append(obj.id)
                    used_identifiers.append(identifier)
                except Exception, e:
                    self._save_gather_error('Error for the identifier %s [%r]' % (identifier,e), harvest_job)
                    continue

        except Exception, e:
            self._save_gather_error('Error gathering the identifiers from the CSW server [%r]' % e, harvest_job)
            return None

        if len(ids) == 0:
            self._save_gather_error('No records received from the CSW server', harvest_job)
            return None

        return ids

    def fetch_stage(self,harvest_object):
        url = harvest_object.source.url
        # Setup CSW client
        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_object_error('Error contacting the CSW server: %s' % e,harvest_object)
            return False


        identifier = harvest_object.guid
        try:
            # TODO: investigate support for both gmd:MD_Metadata or gmi:MI_Metadata
            record = self.csw.getrecordbyid([identifier])
        except Exception, e:
            self._save_object_error('Error getting the CSW record with GUID %s' % identifier,harvest_object)
            return False

        if record is None:
            self._save_object_error('Empty record for GUID %s' % identifier,harvest_object)
            return False

        try:
            # Save the fetch contents in the HarvestObject
            harvest_object.content = record['xml']
            harvest_object.save()
        except Exception,e:
            self._save_object_error('Error saving the harvest object for GUID %s [%r]' % (identifier,e),harvest_object)
            return False

        log.debug('XML content saved (len %s)', len(record['xml']))
        return True

    def import_stage(self, harvest_object):
        '''Import stage of the OGPD Harvester'''

        log = logging.getLogger(__name__ + '.import')
        #log.debug('Import stage for harvest object: %r', harvest_object)
        log.debug('Import stage for harvest object')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        # Save a reference
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False
        try:
            self.import_inspire_object(harvest_object.content,harvest_object)
            return True
        except Exception, e:
            log.error('Exception during import: %s' % text_traceback())
            if not str(e).strip():
                self._save_object_error('Error importing INSPIRE document.', harvest_object, 'Import')
            else:
                self._save_object_error('Error importing INSPIRE document: %s' % str(e), harvest_object, 'Import')

            if debug_exception_mode:
                raise

    def _setup_csw_client(self, url):
        self.csw = CswService(url)

class DestatisHarvester(GeminiCswHarvester, SingletonPlugin):
    '''
    A Harvester for CSW servers, for targeted at import into the German Open Data Platform now focused on Destatis
    '''
    implements(IHarvester)

    temp_directory = '/temp_destatis_dir'
    def info(self):
        return {
            'name': 'destatis',
            'title': 'Destatis Harvester',
            'description': 'Harvester for CSW Servers, which return a zip file with xml files like Destatis'
            }

    def gather_stage(self,harvest_job):
        log.debug('In DestatisHarvester gather_stage')
        # Get source URL
        url = harvest_job.source.url

        tmpdir = tempfile.gettempdir()
        
        import shutil

        # remove old dir from previous harvest run
        if(os.path.exists(tmpdir+self.temp_directory)):
            shutil.rmtree(tmpdir+self.temp_directory)
        if(os.path.exists(tmpdir+'/file_destatis.zip')):
            os.remove(tmpdir+'/file_destatis.zip')
        try:
            req = urllib2.urlopen(url)
            local_file=open(tmpdir+"/file_destatis.zip", "wb")
            while 1:
                packet = req.read()
                if not packet:
                    break
                local_file.write(packet)
            req.close()
            local_file.close()
        except Exception, e:
            print "error"
            return None
        finally:
            req.close()
            
        import zipfile 
        zipfile.ZipFile(tmpdir+"/file_destatis.zip","r").extractall(tmpdir+self.temp_directory)

        ids = []
        for xml_file in os.listdir(tmpdir+self.temp_directory):
            obj = HarvestObject(guid = None, job = harvest_job)
            obj.save()
            ids.append(obj.id)
            
        if len(ids) == 0:
            self._save_gather_error('No records received from the CSW server', harvest_job)
            return None

        return ids
    
    def fetch_stage(self,harvest_object):
        
        identifier = harvest_object.guid
        tmpdir = tempfile.gettempdir()
        for xml_file in os.listdir(tmpdir+self.temp_directory):
            try:
                f = open(tmpdir + self.temp_directory+'/'+xml_file,"r")
                harvest_object.content = f.read()
                harvest_object.save()
                if(len(harvest_object.content) == 0):
                    self._save_object_error('Empty record for GUID %s' % identifier,harvest_object)
                    return False
                log.debug('XML content saved (len %s)', len(harvest_object.content))
                return True
            except Exception, e:
                self._save_object_error('Error saving the harvest object for GUID %s [%r]' % (identifier,e),harvest_object)
                return False
            finally:
                f.close()

    def import_stage(self, harvest_object):
        '''Import stage of the Destatis Harvester'''

        log = logging.getLogger(__name__ + '.import')
        #log.debug('Import stage for harvest object: %r', harvest_object)
        log.debug('Import stage for harvest object')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        # Save a reference
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False
        try:
            self.import_inspire_object(harvest_object.content,harvest_object)
            return True
        except Exception, e:
            log.error('Exception during import: %s' % text_traceback())
            if not str(e).strip():
                self._save_object_error('Error importing INSPIRE document.', harvest_object, 'Import')
            else:
                self._save_object_error('Error importing INSPIRE document: %s' % str(e), harvest_object, 'Import')

            if debug_exception_mode:
                raise

    def _setup_csw_client(self, url):
        self.csw = CswService(url)
        
    def handle_resources(self,resource_locators):
        ''' Handle all resources except for WMS endpoints for Destatis '''
        result = []
        if len(resource_locators):
            log.info("Found %s resources" %len(resource_locators))
            for resource_locator in resource_locators:
                print "Resource: ",
                print resource_locator
                url = resource_locator.get('url','')
                if url:
                    resource_format = ''
                    resource = {}
                    if self._is_pdf_URI(url):
                        resource_format = 'PDF'
                    elif self._is_wms(url):
                        resource['verified'] = True
                        resource['verified_date'] = datetime.now().isoformat()
                        resource_format = 'WMS'
                    elif 'tabelleErgebnis' in url:
                        log.info("Found XLS resource")
                        resource_format = 'XLS'
                        url = url.replace('tabelleErgebnis','tabelleDownload') + '.xls'
                        
                    resource.update(
                        {
                            'url': url,
                            'name': resource_format + ' - Ressource',
                            'description': resource_format + ' - Ressource',
                            'format': resource_format or None,
                            'resource_locator_protocol': resource_locator.get('protocol',''),
                            'resource_locator_function':resource_locator.get('function','')

                        })
                    result.append(resource)
        return result

    def handle_licenses(self, gemini_values):
        # add cc-by to gemini other constraints
        gemini_values['other-constraints'].append('CC-BY 3.0')
        return ckanext.spatial.lib.license_map.translate_license_data(gemini_values)