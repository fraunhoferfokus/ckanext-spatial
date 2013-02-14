#!/usr/bin/python
# -*- coding: utf-8 -*-
from urlparse import urlparse

import unittest

def extract_license_url(constraints):
        '''
        Extracts the first url from other-constraints if there is one
        '''

        for constraint in constraints:
                o = urlparse(constraint)
                if o.scheme and o.netloc:
                        return constraint
        return None

def translate_license_data(gemini):
        '''
        Maps the legal constraints from INSPIRE to the license fields
        specified in the OGD metadata schema.
        '''

        # Default values
        terms_of_use = { 'license_id' : 'other-closed',
                         'license_url' : None,
                         'other' : None }

        # English-German translation for MD_RestrictionCodes
        code_translation = { 'copyright'                    : 'Urheberrecht',
                             'patent'                       : 'Patent',
                             'patentPending'                : 'Patent angemeldet',
                             'patent pending'               : 'Patent angemeldet',
                             'trademark'                    : 'Warenzeichen',
                             'license'                      : 'Lizenz',
                             'intellectualPropertyRights'   : 'geistiges Eigentum',
                             'intellectual property rights' : 'geistiges Eigentum',
                             'restricted'                   : u'beschräter Zugang',
                             'otherRestrictions'            : u'andere Beschräung' }

        # License adjustments based on information gathered through data mining
        constraint_translation = { 'CC-BY 3.0'               : 'cc-by',
                                   'keine Angaben'           : None,
                                   'conditions unknown'      : None,
                                   'Keine'                   : 'cc-zero',
                                   'none'                    : 'cc-zero',
                                   'free'                    : 'cc-zero',
                                   'keine'                   : 'cc-zero',
                                   'no conditions apply'     : 'cc-zero',
                                   'CC BY-SA'                : 'cc-by-sa',
                                   'Datenlizenz Deutschland' : 'dl-de-by-1.0'
                 }

        # Dictionary to map license IDs to their URLs
        urls = { 'cc-by'   : 'http://creativecommons.org/license/by/3.0/de',
                 'cc-zero' : 'http://creativecommons.org/publicdomain/zero/1.0/deed.de',
         'cc-by-sa': 'http://creativecommons.org/licenses/by-sa/3.0/de',
        'dl-de-by-1.0' : 'http://www.daten-deutschland.de/bibliothek/Datenlizenz_Deutschland/dl-de-by-1.0'
               }

        # Sum up the use limitations and use constraints in the 'other'
        # field of the terms of use
        other = ''
        
        # Licenses were sent according to a fix pattern. Therefore, data mining methods
        # are no longer needed.  
        for constraint in gemini['use-limitations']:
            for word in constraint_translation.keys():
                if word in constraint:
                    terms_of_use['license_id'] = constraint_translation[word]
                    gemini['use-limitations'].remove(constraint) 
                    name = None
                    if 'Namensnennung:' in constraint:
                        name = constraint.split('Namensnennung:')[1]
                        name = 'Namensnennung:' + name.replace('\"', '')
                        
                    terms_of_use['other'] = name
                 

        # Apply data mined license url information
        if urls.has_key(terms_of_use['license_id']):
                terms_of_use['license_url'] = urls[terms_of_use['license_id']]
                
                    
        # If terms of use ID is null drop the entry
        if terms_of_use['license_url'] is None and terms_of_use['other'] is None:
                return None
        else:
                return terms_of_use


class TranslateLicenseTest(unittest.TestCase):

        def test_translate_license_data(self):

                gemini = { 'use-limitations' : [ u'Datenlizenz Deutschland - Namensnennung - Version 1.0; <a href="https://github.com/fraunhoferfokus/ogd-metadata/blob/master/lizenzen/BMI/Datenlizenz_Deutschland_Namensnennung_V1.md">http://www.daten-deutschland.de/bibliothek/Datenlizenz_Deutschland/dl-de-by-1.0</a>; dl-de-by-1.0; Namensnennung: "Freie und Hansestadt Hamburg, Statistisches Amt für Hamburg und Schleswig-Holstein"'  ],
                           'use-constraints' : [],
                           'access-constraints' : [],
                           'other-constraints' : [] }

                result = { 'license_id' : 'dl-de-by-1.0',
                           'license_url' : 'http://www.daten-deutschland.de/bibliothek/Datenlizenz_Deutschland/dl-de-by-1.0',
                           'other' : u'Namensnennung: Freie und Hansestadt Hamburg, Statistisches Amt für Hamburg und Schleswig-Holstein'
                        }

                self.assertEquals(translate_license_data(gemini), result)

if __name__ == '__main__':
        unittest.main()








