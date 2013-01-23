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
        terms_of_use = {'license_id': 'other-closed',
                        'license_url': '',
                        'other': ''}

        # English-German translation for MD_RestrictionCodes
        code_translation = {'copyright':                     'Urheberrecht',
                            'patent':                        'Patent',
                            'patentPending':                 'Patent angemeldet',
                            'patent pending':                'Patent angemeldet',
                            'trademark':                     'Warenzeichen',
                            'license':                       'Lizenz',
                            'intellectualPropertyRights':    'geistiges Eigentum',
                            'intellectual property rights':  'geistiges Eigentum',
                            'restricted':                   u'beschränkter Zugang',
                            'otherRestrictions':            u'andere Beschränkung'}

        # License adjustments based on knowledge gathered through data mining
        constraint_translation = {'CC-BY 3.0':           'cc-by',
                                  'keine Angaben':        None,
                                  'conditions unknown':   None,
                                  'Keine':               'cc-zero',
                                  'none':                'cc-zero',
                                  'free':                'cc-zero',
                                  'keine':               'cc-zero',
                                  'no conditions apply': 'cc-zero'}

        # Dictionary to map license IDs to their URLs
        urls = {'cc-by':   'http://creativecommons.org/license/by/3.0/de',
                'cc-zero': 'http://creativecommons.org/publicdomain/zero/1.0/deed.de'}

        # Sum up the use limitations and use constraints in the 'other'
        # field of the terms of use
        other = ''
        if (len(gemini['use-limitations']) > 0 or len(gemini['use-constraints']) > 0):
                other += u'Nutzungsbeschränkungen: '
                for limitation in gemini['use-limitations']:
                        other += limitation + ' '

        # Append the use constraints if applicable
        if len(gemini['use-constraints']) > 0:
                for constraint in gemini['use-constraints']:
                        other += code_translation[constraint] + ' '

        # Sum up the access constraints in the 'other' field of the
        # terms of use as well
        if len(gemini['access-constraints']) > 0:
                other += u'Weitere Beschränkungen: '
                for constraint in gemini['access-constraints']:

                        if constraint in constraint_translation:
                                other += code_translation[constraint] + ' '
                        else:
                                other += constraint

        # Append the other constraints information
        if len(gemini['other-constraints']) > 0:
                for constraint in gemini['other-constraints']:
                        other += constraint + ' '

        if len(other) > 0:
                other = other[:-1]

        terms_of_use['other'] = other

        # Check if other-constraints is a URL and use it for the license
        # url field if applicable
        for constraint in gemini['other-constraints']:
                if extract_license_url(gemini['other-constraints']):
                        terms_of_use['license_url'] = constraint

        # Apply data mined license information
        for constraint in gemini['other-constraints']:
                if constraint in constraint_translation:
                        terms_of_use['license_id'] = constraint_translation[constraint]
                else:
                        other += constraint

        # Apply data mined license url information
        if terms_of_use['license_id'] in urls:
                terms_of_use['license_url'] = urls[terms_of_use['license_id']]

        # If terms of use ID is null drop the entry
        if terms_of_use['license_id'] is None:
                return None
        else:
                return terms_of_use


class TranslateLicenseTest(unittest.TestCase):

        def test_translate_license_data(self):

                gemini = {'use-limitations':    [],
                          'use-constraints':    [],
                          'access-constraints': [],
                          'other-constraints':  []}

                result = {'license_id':  'other-closed',
                          'license_url': '',
                          'other':       ''}

                self.assertEquals(translate_license_data(gemini), result)

                gemini = {'use-limitations':    [],
                          'use-constraints':    [],
                          'access-constraints': [],
                          'other-constraints':  ['http://www.example.com']}

                result = {'license_id':  'other-closed',
                          'license_url': 'http://www.example.com',
                          'other':       'http://www.example.com'}

                self.assertEquals(translate_license_data(gemini), result)

                gemini = {'use-limitations':    [u'Nur im Maßstab 1:50.000'],
                          'use-constraints':    [],
                          'access-constraints': [],
                          'other-constraints':  []}

                result = {'license_id':   'other-closed',
                          'license_url':  '',
                          'other':       u'Nutzungsbeschränkungen: Nur im Maßstab 1:50.000'}

                self.assertEquals(translate_license_data(gemini), result)

                gemini = {'use-limitations':    [u'Nur im Maßstab 1:50.000'],
                          'use-constraints':    ['copyright'],
                          'access-constraints': [],
                          'other-constraints':  ['http://example-license.com/']}

                result = {'license_id':   'other-closed',
                          'license_url':  'http://example-license.com/',
                          'other':       u'Nutzungsbeschränkungen: Nur ' +
                                         u'im Maßstab 1:50.000 Urheberrecht ' +
                                         'http://example-license.com/'}

                self.assertEquals(translate_license_data(gemini), result)

if __name__ == '__main__':
        unittest.main()
