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
from ckanext.spatial.lib.durationmap import DurationTranslator
from ckanext.spatial.validation import Validators, all_validators

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
debug_exception_mode = bool(os.getenv('DEBUG'))



class SpatialHarvester(object):
    # Q: Why does this not inherit from HarvesterBase in ckanext-harvest?

    def _is_wms(self,url):
        try:
            capabilities_url = wms.WMSCapabilitiesReader().capabilities_url(url)
            res = urllib2.urlopen(capabilities_url,None,10,timeout=10)
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
        http_response = urllib2.urlopen(url,timeout=10)
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

    extent_template = Template('''{"type":"Polygon","coordinates":[[[$minx, $miny],[$minx, $maxy], [$maxx, $maxy], [$maxx, $miny], [$minx, $miny]]]}
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
        #xml = etree.fromstring(gemini_string)

        #valid, messages = self._get_validator().is_valid(xml)
        #if not valid:
        #    log.error('Errors found for object with GUID %s:' % self.obj.guid)
        #    out = messages[0] + ':\n' + '\n'.join(messages[1:])
        #    self._save_object_error(out,self.obj,'Import')

        #unicode_inspire_string = etree.tostring(xml, encoding=unicode, pretty_print=True)

        package = self.write_package_from_inspire_string(gemini_string,harvest_object)

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

        extras = {}

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
            extras['temporal_coverage_from'] = gemini_values['temporal-extent-begin']
        if gemini_values.has_key('temporal-extent-end'):
            extras['temporal_coverage_to'] = gemini_values['temporal-extent-end']

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
            req = urllib2.urlopen(url,timeout=10)
            headers = req.headers['content-type']
            #TODO application/octet-stream isbinary, accept as pdf?
            return 'application/pdf' in headers
    
    def _is_htm_or_html(self, url):
        if url.endswith('.htm') or url.endswith('html'):
            log.info("%s is html page" %url)
            return True
        else:
            log.info('try to open url: %s' %url)
            req = urllib2.urlopen(url,timeout=10)
            headers = req.headers['content-type']
            if 'text/html' in headers:
                log.info("%s is html page" %url)
                return True
    
    
    def _is_probably_wms(self, url):
        log.info('try to open url: %s' %url)
        try:
            urllib2.urlopen(url,timeout=10)
        except HTTPError :
            return False
        else:
            # status code is 200
            # accept this URL as WMS
            return True
    
    def _is_zib(self, url):
        if url.endswith('.zip'):
            log.info("%s is zip file" %url)
            return True
        else:
            return False
    
    def write_package_from_inspire_string(self, content, harvest_object):
        '''Create or update a package based on fetched INSPIRE content'''
        
        log = logging.getLogger(__name__ + '.import')
        package = None
        
        #gemini_document = InspireDocument(content)
        #gemini_values = gemini_document.read_values()               

        # parse it to valid xml
        xml = etree.fromstring(content)        
        unicode_gemini_string = etree.tostring(xml, encoding=unicode, pretty_print=True)
        
        # parse it to a inspire document
        gemini_document = InspireDocument(unicode_gemini_string)
        gemini_values = gemini_document.read_values()

        gemini_guid = gemini_values['guid']
        
        self.related_data_ids.append(gemini_guid)
        #log.debug('RELATIONSHIP_PACKAGE: %s', gemini_guid)        

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
                        #log.info('Package for object with GUID %s will be re-activated' % gemini_guid)
                        reactivate_package = True
                    else:
                         #log.info('Remote record with GUID %s is not more recent than a deleted package, skipping... ' % gemini_guid)
                         return None

            else:
                #log.info('Document with GUID %s unchanged, skipping...' % (gemini_guid))
                return None
        else:
            log.info('No package with INSPIRE guid %s found, let''s create one' % gemini_guid)

        extras = {}

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
                    temp_duration = 'tag'
                    temp_factor = 14   
            else:
                if gemini_values['frequency-of-update'] == 'biannually':
                    temp_duration = 'monat'
                    temp_factor = 6            
                else:
                    if duration:       
                        temp_duration = duration['duration']
                        temp_factor =  duration['duration_factor']                       
        
        if temp_duration:
            extras['temporal_granularity'] = temp_duration 
        if temp_factor:
            extras['temporal_granularity_factor'] = temp_factor
                        
       
        #map given dates to OGPD date fields
              
        dates = []    
        
        release_dates = self.get_dates(gemini_values['date-released'], u'veroeffentlicht')
        creation_dates = self.get_dates(gemini_values['date-created'], u'erstellt')
        update_dates = self.get_dates(gemini_values['date-updated'], u'aktualisiert')
        
        dates = release_dates + creation_dates + update_dates
             
        extras['dates']= dates 
 
 
 
        extras['subgroups'] = gemini_values['topic-category']
        log.debug('Set subgroups: ' + str(gemini_values['topic-category']))



        if len(gemini_values['temporal-extent-begin']) > 0:
            extras['temporal_coverage_from'] =  self.get_datetime(gemini_values['temporal-extent-begin'][0])
        if len(gemini_values['temporal-extent-end']) > 0:
            extras['temporal_coverage_to'] =  self.get_datetime(gemini_values['temporal-extent-end'][0])


        # map INSPIRE constraint fields to OGPD license fields
        terms_of_use = ckanext.spatial.lib.license_map.translate_license_data(gemini_values)

        # terms of use == null indicates to drop the entry completely
        if terms_of_use is None:
                log.error('Package with GUID %s does not contain any licenses, skip this package' % self.obj.guid)
                return None

        extras['terms_of_use'] = terms_of_use


        # map INSPIRE responsible organisation fields to OGPD contacts
        roles = ['publisher', 'owner', 'author', 'distributor', 'pointOfContact', 'resourceProvider']
        contacts = []
        for role in roles:
            contact = {}
            if gemini_values.has_key(role + '-email') and gemini_values[role + '-email']: 
                contact['email'] = gemini_values[role + '-email']
              
            if gemini_values.has_key(role + '-url') and gemini_values[role + '-url']: 
                contact['url'] = gemini_values[role + '-url']
    
            if gemini_values.has_key(role + '-individual-name'):  
                if gemini_values[role + '-individual-name']:
                    contact['name'] = gemini_values[role + '-individual-name']
                else:
                    if gemini_values.has_key(role + '-organisation-name'):
                        contact['name'] = gemini_values[role + '-organisation-name']
                    else:
                        if gemini_values.has_key(role + '-position-name'):
                            contact['name'] = gemini_values[role + '-position-name']
                    
            if gemini_values.has_key(role + '-address') and gemini_values[role + '-address']:
                contact['address'] = gemini_values[role + '-address']


            if len(contact) != 0:                    
                if role == 'pointOfContact':
                    contact['role'] = 'ansprechpartner'   
                elif role == 'publisher':
                    contact['role'] = 'veroeffentlichende_stelle' 
                    if gemini_values['publisher-organisation-name']:
                                contact['name'] = gemini_values['publisher-organisation-name']  
                elif role == 'owner' or role == 'author':
                    contact['role'] = 'autor'   
                elif role == 'distributor' or role == 'resourceProvider':
                    contact['role'] = 'vertrieb'       
                     
                contacts.append(contact)   
              
        extras['contacts'] = contacts

        # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry
      
        extent_string = self.extent_template.substitute(
                minx = gemini_values['bbox-east-long'],
                miny = gemini_values['bbox-south-lat'],
                maxx = gemini_values['bbox-west-long'],
                maxy = gemini_values['bbox-north-lat']
                )

        extras['spatial'] = extent_string.strip()
        
    


        if gemini_values.has_key('spatial-text'): 
            extras['spatial-text'] = gemini_values['spatial-text']

        extras['geographical_granularity'] = 'stadt'
        
        
        
        tags2 = []
        for key in ['keyword-inspire-theme', 'keyword-controlled-other', 'keyword-free-text']:
            for item in gemini_values[key]:
                if item not in tags2:
                    tags2.append(item)

        # Only [a-zA-Z0-9-_] is allowed, filter every other character
        tags = []

        if 'opendata' in tags2 or '#opendata_hh#' in tags2:
             for tag in tags2:
                    if tag != 'opendata' and tag != '#opendata_hh#' and tag not in gemini_values['groups']:
                        tag = tag[:50] if len(tag) > 50 else tag
                        tag = unicode(tag)
                        tags.append({'name':tag})         
        else:
            return None

        
        #add groups (mapped from ISO 19115 into OGD schema) 
        groups = []
        groups_in_database = Session.query(model.Group.name).filter(model.Group.state == 'active')
        
        
        for group in gemini_values['groups']:
            
            group = self.translate_group(group)
            
            if group:
                for group_in_database in groups_in_database.all():           
                    if group in group_in_database:
                        groups.append({'name':group_in_database.name} )
            

        package_dict = {
            'title': gemini_values['title'],
            'notes': gemini_values['abstract'],
            'tags': tags,
            'groups': groups,
            'resources' :[]          
        }
        
         # extract in applications and services all given references to used datasets     
                
        used_datasets = []
        if len(gemini_values['used_datasets']) > 0:
            for uri in gemini_values['used_datasets']:
                csw_elements = uri.split('&')
                for element in csw_elements:
                    if 'id' in element:
                        used_datasets.append((element.split('='))[1]) 
                                   
        if len(gemini_values['coupled-resource']) > 0:
            used_dataset_url = 'http://gateway.hamburg.de/OGCFassade/HH_CSW.aspx?Service=CSW&Request=GetRecordById&Version=2.0.2&outputSchema=http://www.isotc211.org/2005/gmd&elementSetName=full'
            for resource in gemini_values['coupled-resource']:
                used_datasets.append(resource['uuid'][0])
                
                
        used_datasets_id = []
        
        
        harvest_job = harvest_object.job
        for dataset in used_datasets:          
            
            name_title = self.gen_new_name(gemini_values['title'])
            slug = None
            slug = self.harvest_individual_data(dataset,harvest_job)
            
            if not(slug is None):
               
                used_datasets_id.append(slug)


        extras['sector'] = 'oeffentlich'


        if len(used_datasets_id) > 0:
            extras['used_datasets'] = used_datasets_id         
        
        
        Session.expunge_all()
        last_harvested_object = Session.query(HarvestObject) \
                            .filter(HarvestObject.guid==gemini_guid) \
                            .filter(HarvestObject.current==True) \

        self.obj = harvest_object
        self.obj.save()
        
        '''
        #set the core ckan fields maintainer and author
        #package_dict['author'] = 'ÖÄAS'  
        package_dict['author_email'] = 'test@test.com'
        
        test = u'öäas'
        package_dict['maintainer'] = unicode(test)  
        package_dict['maintainer_email'] ='test@test.com'
         
        '''
        for contact in contacts:
            if contact['role'] == 'veroeffentlichende_stelle':
                package_dict['author'] = unicode(contact['name'])    
                package_dict['author_email'] = contact['email']
            if contact['role'] == 'ansprechpartner':
                package_dict['maintainer'] = unicode(contact['name'])    
                package_dict['maintainer_email'] = contact['email']
        
       
        #type of the dataset 
        if 'application' in gemini_values['resource-type'] or 'service' in gemini_values['resource-type'] :
            package_dict['type'] = 'app'      
        else:
            package_dict['type'] = 'datensatz'

        
        
        #set url for further information
        if gemini_values.has_key('further_info'):
            package_dict['url'] = gemini_values['further_info']
            
        
        #copy license_id to ckan-core license id
        package_dict['license_id'] = extras['terms_of_use']['license_id']
        #log.debug('Set license_id to %s' %package_dict['license_id'])
        
        #log.debug('Set groups to ' + str(package_dict['groups']))

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
        service_locators = gemini_values.get('service-locator', [])
        
        # extract found services from resources and put them to the services
        result_set = self.find_services_in_resources(service_locators, resource_locators)
        resource_locators = result_set ['resources'] 
        service_locators = result_set['services']
        
        # get all formats from the inspire file
        formats = []
        formats = self.get_data_format_from_inspire(gemini_values['data-format'] )
        used_formats = []
        contains_wfs = False
        format_is_set = False
        
        
        if len(service_locators):
            #log.info("Found %s service points" % len(resource_locators))
            for service_locator in service_locators:
                url = service_locator.get('url', '')
                if url:
                    if self._is_valid_URL(url):
                        service_format = ''
                        resource = {}
                        
                        service_des = service_locator.get('description')
                        service_format = None
                        if service_des:                           
                            if 'Format:' in service_des:
                                    format_is_set = True
                                    for service in self.service_formats:
                                        if service in service_des.upper():
                                            service_format = service
                            else:      
                                service_format = self.get_service_name_from_url(url)
                        else:      
                            service_format = self.get_service_name_from_url(url)
                        
                        resource['verified'] = True
                        
                        if service_format == 'WFS':
                            contains_wfs = True
                        
                        resource.update(
                            {
                                'url': url,
                                'name': service_locator.get('name',''),
                                'description':  service_locator.get('description') if service_locator.get('description') else 'Ressource',
                                'format':service_format or None,
                                'type' : 'api',
                                'resource_locator_protocol': service_locator.get('protocol',''),
                                'resource_locator_function':service_locator.get('function','')
    
                            })
                        
                        if not self.is_similar_url_in_services(url, service_locators) and not self.is_url_in_services(url, package_dict['resources']) and resource not in package_dict['resources']: 
                            package_dict['resources'].append(resource)
                            
                           
        if not format_is_set:
            package_dict['resources'] = self.match_service_format(package_dict['resources'], formats, used_formats, contains_wfs)
        
        
        format_is_set = False
        if len(resource_locators):
            
            #log.info("Found %s resources" %len(resource_locators))
            for resource_locator in resource_locators:
                url = resource_locator.get('url','')                
                if url:
                    if self._is_valid_URL(url):
                        resource_format = ''
                        resource = {}
                           
                        #set the language of the resource 
                        if  len(gemini_values['dataset-language'] ) > 0:  
                            resource['language'] = gemini_values['dataset-language'][0]         
                        
                        resource_format = ''
                        resource_des = resource_locator.get('description')
                        if resource_des:
                            if 'Format:' in resource_des:
                                    format_is_set = True
                                    for resource in self.resource_formats:
                                        if resource in resource_des.upper():
                                            resource_format = resource
                            else:       
                                resource_format = self.get_data_format_from_url(url)   
                        else:
                            resource_format = self.get_data_format_from_url(url)    
                        
                         
                        resource.update(
                            {
                                'url': url,
                                'name': resource_locator.get('name',''),
                                'description': resource_locator.get('description') if resource_locator.get('description') else 'Ressource',
                                'format': resource_format or None,
                                'type' : 'file',
                                'resource_locator_protocol': resource_locator.get('protocol',''),
                                'resource_locator_function':resource_locator.get('function','')
    
                            })
                        package_dict['resources'].append(resource)  
            
        if not format_is_set:                   
            package_dict['resources'] = self.match_resource_format(package_dict['resources'], formats, used_formats)

            

            # Guess the best view service to use in WMS preview
            verified_view_resources = [r for r in package_dict['resources'] if 'verified' in r and r['format'] == 'WMS']
            if len(verified_view_resources):
                verified_view_resources[0]['ckan_recommended_wms_preview'] = True
            else:
                view_resources = [r for r in package_dict['resources'] if r['format'] == 'WMS']
                if len(view_resources):
                    view_resources[0]['ckan_recommended_wms_preview'] = True
                    
       
       
       
       
       

            # Guess the best view service to use in WMS preview
            verified_view_resources = [r for r in package_dict['resources'] if 'verified' in r and r['format'] == 'WMS']
            if len(verified_view_resources):
                verified_view_resources[0]['ckan_recommended_wms_preview'] = True
            else:
                view_resources = [r for r in package_dict['resources'] if r['format'] == 'WMS']
                if len(view_resources):
                    view_resources[0]['ckan_recommended_wms_preview'] = True
                    
                    
        #original metadata information  
        url_schema = urlparse(harvest_object.source.url)
        extras['metadata_original_portal'] = url_schema.netloc
            
       
        extras['metadata_original_id'] = gemini_values['guid']
        
        csw_request = '?Service=CSW&ElementSetName=full&Request=GetRecordById&Id='
        extras['metadata_original_xml'] = harvest_object.source.url + csw_request + gemini_values['guid']
        
        extras['ogd_version'] = OGPDHarvester.version
        
        extras['sector'] = 'oeffentlich'
        

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
            
            if package == None:
                # Create new package from data.
                package = self._create_package_from_data(package_dict)
                #log.info('Created new package ID %s with GEMINI guid %s', package['id'], gemini_guid)
            else:
                package = self._create_package_from_data(package_dict, package = package)
                #log.info('Updated existing package ID %s with existing GEMINI guid %s', package['id'], gemini_guid)
    
    
            # add relationships between datasets
            theparent = model.Package.by_name(name=package_dict['name'])
            for d in used_datasets_id:
                thechild = model.Package.by_name(name=d)
                rev = model.repo.new_revision()
                theparent.add_relationship(u'links_to', thechild, u'basisdaten')
                r = thechild.relationships_as_subject
                model.repo.commit_and_remove()
   

            for slug in used_datasets_id:
               
                url = ''
                like_q = u'%s%%' % slug
                pkg_query = Session.query(Package).filter(Package.name.ilike(like_q))
                for pkg in pkg_query:
                    if pkg.name == slug:
                        url = pkg.url
                        break
                
                related_dict = {
                    'title': 'basisdaten',
                    'type': 'Idea',
                    'dataset_id ': package['id'],
                    'url' : url           
                }
                
                result = self._create_related_item(related_dict)
                

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
            
            
            if not self.obj.package:
                self.obj.package = package
            
            
            self.obj.current = True
            self.obj.save()
    
            #print 'End : ############################################################################################'

            return package
       
   
    def _is_valid_URL(self,url):
        '''
        This method checks whether the given url has a 
        valid url scheme and a valid netloc 
        '''
        
        url_schema = urlparse(url)
        if url_schema.netloc and url_schema.scheme:
            return True
        return False
    
    
    def is_url_in_services(self, first_url, services):
        '''
        This method checks whether the given url is already in the list of services.
        '''
        first_url_schema = urlparse(first_url)  
        
        if len(services):
            for service in services:
                second_url = service['url']        
                second_url_schema = urlparse(second_url)
                
                first_query = first_url_schema.query.split('&')
                second_query = second_url_schema.query.split('&')
                
                # service requests which differ in their order of parameteres in its query were extracted 
                if set(second_query) == set(first_query) and first_url_schema.scheme == second_url_schema.scheme and first_url_schema.path == second_url_schema.path:
                    return True
                
                return False
    
    def is_similar_url_in_services(self,url,services):
        '''
        This method checks whether a similar version of a url is 
        already presented in the list of services.
        '''
        url_schema = urlparse(url)
        if len(services):
            for service in services:
                current_url = service.get('url', '')
                if current_url != url:
                    current_url_schema = urlparse(current_url)
        
                    if current_url_schema.path == url_schema.path and not url_schema.query or url_schema.query.startswith('version='):
                        return True
        return False
  
    def find_services_in_resources(self,services, resources):
        '''
        This method extracts all services which were defined in the inspire resource field
        and puts them to right list.
        '''
        result = {}
        found_services = []
        if len(resources):
            for resource in resources:
                url = resource.get('url','') 
                if 'SERVICE=' in url.upper():
                    found_services.append(resource)
                    #services.append(resource) 
                    #resources.remove(resource)
                    
        for service in found_services:
            services.append(service) 
            resources.remove(service)
            
        result['resources'] = resources
        result['services'] = services
        return result
        
  
    def match_service_format(self, services, formats, used_formats, contains_wfs):
        '''
        This method searchs for each service a suitable format from the format list
        which is extracted from the inspire document.
        '''
        for service in services:
            if service['format']:
                if 'WFS' in service['format']:
                    if 'GML' in formats or 'GML' in used_formats:
                        service['format'] = self.validate_resource('GML')
                        try:
                            formats.remove('GML')
                        except:
                            print 'GML'
                        used_formats.append('GML')
                elif 'WMS' in service['format'] and 'GML' in formats and not contains_wfs:
                    service['format'] = self.validate_resource('GML')
                    formats.remove('GML')
                    used_formats.append('GML')
            elif 'GML' in formats:
                service['format'] = self.validate_resource('GML')
                formats.remove('GML')
                used_formats.append('GML')
            
        return services
            
  
    def validate_resource(self,resource_format):
        '''
        Returns the given resource if the format list contains the format 
        of the resource. Otherwise it returns an empty string.
        '''
        if resource_format in self.resource_formats or resource_format in self.service_formats:
            return resource_format 
        return ''
    
  
    def match_resource_format(self, resources, formats, used_formats):
        '''
        This method searchs for each resource a suitable format from the format list
        which is extracted from the inspire document.
        '''
        import copy
        found_formats = []
        for resource in resources:
            if resource['format'] in formats:
                found_formats.append(resource['format'])
        
        for f in found_formats:
            try:
                formats.remove(f)
                used_formats.append(f)
            except:
                print f
        
        copied_resources = []
        if len(formats)>0:
                for f in formats:
                    if 'TIF' in f:
                        for resource in resources:
                            if resource['format']:
                                if resource['format'] == 'JPEG':
                                    resource_copy = copy.deepcopy(resource)
                                    resource_copy['format'] = 'TIFF'
                                    copied_resources.append(resource_copy)
                                    resource['format'] = self.validate_resource('TIF')
                           
                    else:                    
                        for resource in resources:
                            if resource['format']:              
                                if 'ZIP' in resource['format'] or 'WEB' in resource['format'] or 'HTML' in resource['format']:
                                    resource['format'] = self.validate_resource(f)
                            else:
                                resource['format'] = self.validate_resource(f)
                                    
                            
        for resource in copied_resources:
                    resources.append(resource) 
                   
        return resources 
  
    def get_service_name_from_url(self, url):
        '''
         This method tries to find the service name.
        '''
        import string 
        service = None
        
        url_upper = string.upper(url)
        if 'SERVICE=WFS' in url_upper:
            service = 'WFS'
        elif 'SERVICE=WMS' in url_upper:
            service = 'WMS' 
        elif 'WMSSERVER' in url_upper:
            service = 'WMS'
        elif url.endswith('.zip'):
            service = 'ZIP'

        if service:
            return self.validate_resource(service)
        
        return None
  
    def get_data_format_from_inspire(self, data_formats):
        '''
         This method tries to relate all formats in the inspire document to a suitable mime type.
        '''
        formats = []
        for f in data_formats:
            if f['name'].isupper():
                formats.append(f['name'])
            else:
                if 'GML' in f['name']:
                    formats.append('GML')
        return formats
    
    
    def get_data_format_from_url(self, url):
        '''
        This method assigns a mime type to the given url according to its name.
        '''
        resource = ''

        if 'ascii' in url:
            resource = 'ASCII'
        elif 'excel' in url:
            resource ='XLS'
        elif url.endswith(".pdf"):
            resource = 'PDF'
        elif url.endswith(".zip"):
            resource = 'ZIP'
        elif url.endswith('.htm') or url.endswith('.html') or '.htm' in url or '.html' in url:
            resource = 'HTML'
        elif url.endswith('.jpg'):
            resource = 'JPEG'
        elif url.endswith('.xls'):
            resource = 'XLS'
        elif url.endswith('.txt'):
            resource = 'TXT'
        else:
            resource = 'WEB'
        
        return self.validate_resource(resource)
   


    def harvest_individual_data(self,id,harvest_job):               
        
        #log.debug('RELATIONSHIP_REQUEST: %s', id)
    
        if id in self.related_data_ids:
                
                return None  

        harvested_object = Session.query(HarvestObject) \
                    .filter(HarvestObject.guid==id) \
                    .all()

        if len(harvested_object) > 0:             
            obj = harvested_object[0]
            self.obj = obj 
            self.obj.save() 
        
            try:
                    Session.expunge_all()
                    Session.add(self.obj)
                    Session.refresh(self.obj)
            except:
                Session.merge(self.obj)
                #log.debug('error occurend while updating the session')

            
            if obj.package:
                packages = Session.query(model.Package.name).autoflush(False).filter_by(name=obj.package.name)
                result = packages.first()
                
                if result:
                    return result[0]       
                else:
                    package = self.write_package_from_inspire_string(obj.content, obj)
        
                    if package is None:
                        return None
                    else:                    
                        return package['name']     
                    
            else:     
                package = self.write_package_from_inspire_string(obj.content, obj)
    
                if package is None:
                    return None
                else:
                    return package['name']    
 
        else:       
     
            csw = CswService('http://hmdk.de/csw')
            #csw = CswService('http://gateway.hamburg.de/OGCFassade/HH_CSW.aspx')
            data = csw.getrecordbyid([id])
            
            if data is None:
                return None
            else:
                obj = HarvestObject(guid = id, job = harvest_job)
                content = data['xml']
                obj.content = content
                obj.save()
    
                self.obj = obj  
                self.obj.save()
                
                try:
                        Session.expunge_all()
                        Session.add(self.obj)
                        Session.refresh(self.obj)
                except:
                        Session.merge(self.obj)
                        #log.debug('error occurend while updating the session')
            

                package = self.write_package_from_inspire_string(obj.content, obj)
            
                if package is None:
                     return None
                else:
                    return package['name']   
            
    
    
    def get_dates(self, dates, role):
        
        result = []   
         
        if len(dates) > 0:
            for date in dates :
                if isinstance(date, basestring):
                    ogpd_date_released =  { 'role' : role, 'date' : self.get_datetime(date)}
                    result.append(ogpd_date_released)
                else:    
                    ogpd_date_released =  { 'role' : role, 'date' : self.get_datetime(date[0])}
                    result.append(ogpd_date_released)    
        return result

      
      
             
    def get_datetime(self,dt):
        
        import datetime         
        try:
            # the given format is already datetime
            t = datetime.datetime.strptime( dt, "%Y-%m-%dT%H:%M:%S" )
            return t.isoformat()
        except:
            # convert date to datetime by adding midnight-time to the date
            d = datetime.datetime.strptime( dt, "%Y-%m-%d")       
            midnight = datetime.time(0)
            return (datetime.datetime.combine(d.date(), midnight)).isoformat()
    
    
    
    
    
    def translate_group(self, group):
        
        map = {u'Bevörung'                                     : 'bevoelkerung',
                'Bildung und Wissenschaft'                     : 'bildung-wissenschaft',
                'Geographie, Geologie und Geobasisdaten'       : 'geo',
                'Gesetze und Justiz'                           : 'gesetze-justiz',
                'Gesundheit'                                   : 'gesundheit',
                'Infrastruktur, Bauen und Wohnen'              : 'infrastruktur-bauen-wohnen',
                'Kultur, Freizeit, Sport und Tourismus'        : 'kultur-freizeit-sport-tourismus',
                'Politik und Wahlen'                           : 'politik-wahlen',
                'Soziales'                                     : 'soziales',
                'Transport und Verkehr'                        : 'transport-verkehr',
                'Umwelt und Klima'                             : 'umwelt-klima',
                'Verbraucherschutz'                            : 'verbraucherschutz',
                'Wirtschaft und Arbeit'                        : 'wirtschaft-arbeit'
              }
  
        out = None
        if group in map.keys():
            out = map[group]
        else:
            if 'Verwaltung' in group:
                out = 'verwaltung'
            else:
                if 'lkerung' in group:
                    out = 'bevoelkerung' 
        return out



    def gen_new_name(self, title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        like_q = u'%s%%' % name
        pkg_query = Session.query(Package).filter(Package.name.ilike(like_q)).limit(100)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 101:
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
  
    
    def _create_related_item(self, related_dict):
        
        context = {'model':model,
                   'session':Session,
                   'user':'harvest',
                   #'schema':package_schema,
                   'extras_as_string':True,
                   'api_version': '2'}
        
        try:
            action_function = get_action('related_create')      
            related_dict = action_function(context, related_dict)
        except ValidationError,e:
            raise Exception('Validation Error: %s' % str(e.error_summary))
            if debug_exception_mode:
                raise

        return related_dict
    

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
        
        group_schema = logic.schema.default_group_schema()
        group_schema['name'] = [not_empty,unicode]
        package_schema['groups'] = group_schema

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
        #log = logging.getLogger(__name__ + '.CSW.gather')
        #log.debug('GeminiCswHarvester gather_stage for job: %r', harvest_job)
        # Get source URL
        url = harvest_job.source.url

        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_gather_error('Error contacting the CSW server: %s' % e, harvest_job)
            return None


        #log.debug('Starting gathering for %s' % url)
        used_identifiers = []
        ids = []
        try:
            for identifier in self.csw.getidentifiers(page=10):
                try:
                    #log.info('Got identifier %s from the CSW', identifier)
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
        #log = logging.getLogger(__name__ + '.CSW.fetch')
        #log.debug('GeminiCswHarvester fetch_stage for object: %r', harvest_object)

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

        #log.debug('XML content saved (len %s)', len(record['xml']))
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
        #log = logging.getLogger(__name__ + '.individual.gather')
        #log.debug('GeminiDocHarvester gather_stage for job: %r', harvest_job)

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

                #log.info('Got GUID %s' % gemini_guid)
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
        #log = logging.getLogger(__name__ + '.WAF.gather')
        #log.debug('GeminiWafHarvester gather_stage for job: %r', harvest_job)

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
                            #log.debug('Got GUID %s' % gemini_guid)
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
    
    job = None
    related_data_ids=[]
    version = 'v1.0'
    resource_formats = [] 
    service_formats = []
    
    def info(self):
        return {
            'name': 'ogpd',
            'title': 'OGPD Harvester',
            'description': 'Harvester for CSW Servers like GDI Geodatenkatalog'
            }

    
    def read_formats(self):
            
        import json
        import os
        try:
            dataset_formats = open('/opt/ckan/pyenv/src/ckan/formats/dataset_formats.json', 'r')
            self.resource_formats = json.loads(dataset_formats.read())
            dataset_formats.close()    
        except Exception, e:
            log.error('Error occurred while reading dataset formats: %r' %(os.getcwd()))   
        try:  
            app_formats = open('/opt/ckan/pyenv/src/ckan/formats/application_formats.json', 'r')
            self.service_formats = json.loads(app_formats.read())
            app_formats.close()
        
        except Exception, e:
            log.error('Error occurred while reading application formats %r' %(os.getcwd()))
    
    
    def gather_stage(self,harvest_job):
        log.debug('In OGPDHarvester gather_stage')
        # Get source URL
        url = harvest_job.source.url
        self.job = harvest_job 
        
        # Setup CSW client
        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_gather_error('Error contacting the CSW server: %s' % e,harvest_job)
            return None


        #log.debug('Starting gathering for %s ' % url)
        used_identifiers = []
        ids = []
        try:
            for record in self.csw.getidentifiers(keywords=['#opendata_hh#'], page=10):
                try:
                    identifier = record['identifier']
                    #log.info('Got identifier %s from the CSW', identifier)
                    if identifier in used_identifiers:
                        log.error('CSW identifier %r already used, skipping...' % identifier)
                        continue
                    if identifier is None:
                        log.error('CSW returned identifier %r, skipping...' % identifier)
                        ## log an error here? happens with the dutch data
                        continue

                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = identifier, job = harvest_job)
                    obj.content = record['xml']
                    print obj.content
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
        # The fetching was already done in the previous stage
        return True
    
    
    
    def import_stage(self, harvest_object):
        '''Import stage of the OGPD Harvester'''

        #log = logging.getLogger(__name__ + '.import')
        #log.debug('Import stage for harvest object: %r', harvest_object)
        #log.debug('Import stage for harvest object')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        self.read_formats()
        
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








