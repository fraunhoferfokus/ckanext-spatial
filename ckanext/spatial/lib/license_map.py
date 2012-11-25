import unittest

def translate_license_data(gemini):
        '''
        Maps the legal constraints from INSPIRE to the license fields
        specified in the OGD metadata schema.
        '''

        # Default values
        terms_of_use = { 'id' : 'other-closed',
                         'license_url' : '',
                         'other' : '' }

        # English-German translation for MD_RestrictionCodes
        code_translation = { 'copyright'                  : 'Urheberrecht',
                             'patent'                     : 'Patent',
                             'patentPending'              : 'Patent angemeldet',
                             'trademark'                  : 'Warenzeichen',
                             'license'                    : 'Lizenz',
                             'intellectualPropertyRights' : 'geistiges Eigentum',
                             'restricted'                 : 'beschr&Auml;nkter Zugang',
                             'otherRestrictions'          : 'andere Beschr&Auml;nkung' }

        # License adjustments based on information gathered through data mining
        constraint_translation = { 'CC-BY 3.0'           : 'cc-by',
                                   'keine Angaben'       : None,
                                   'conditions unknown'  : None,
                                   'Keine'               : 'cc-zero',
                                   'none'                : 'cc-zero',
                                   'free'                : 'cc-zero',
                                   'keine'               : 'cc-zero',
                                   'no conditions apply' : 'cc-zero' }

        # Dictionary to map license IDs to their URLs
        urls = { 'cc-by' : 'http://creativecommons.org/license/by/3.0/de' }

        # Sum up the use limitations and use constraints in the 'other'
        # field of the terms of use
        other = ''
        if (len(gemini['use-limitations']) > 0 or len(gemini['use-constraints']) > 0):
                other += 'Nutzungsbeschr&Auml;nkungen: '
                for limitation in gemini['use-limitations']:
                        other += limitation + ' '
        
        # Append the use constraints if applicable
        if len(gemini['use-constraints']) > 0:
                for constraint in gemini['use-constraints']:
                        other += code_translation[constraint] + ' '

        # Sum up the access constraints in the 'other' field of the
        # terms of use as well
        if len(gemini['access-constraints']) > 0:
                other += 'Weitere Beschr&Auml;nkungen: '
                for constraint in gemini['use-constraints']:
                        other += code_translation[constraint] + ' '

        # Append the other constraints information
        if len(gemini['other-constraints']) > 0:
                for constraint in gemini['other-constraints']:
                        other += constraint + ' '

        if len(other) > 0:
                other = other[:-1]

        # Check if other-constraints is a URL and use it for the license
        # url field if applicable
        for constraint in gemini['other-constraints']:
                if constraint.startswith('http://') or constraint.startswith('https://'):
                        terms_of_use['license_url'] = constraint

        # Apply data mined license information
        for constraint in gemini['other-constraints']:
                if constraint_translation.has_key(constraint):
                        terms_of_use['id'] = constraint_translation[constraint]

        # Apply data mined license url information
        if urls.has_key(terms_of_use['id']):
                terms_of_use['license_url'] = urls[terms_of_use['id']]

        # If terms of use ID is null drop the entry
        if terms_of_use['id'] is None:
                return None
        else:
                return terms_of_use


class TranslateLicenseTest(unittest.TestCase):

        def test_translate_license_data(self):

                gemini = { 'use-limitations' : [],
                           'use-constraints' : [],
                           'access-constraints' : [],
                           'other-constraints' : [] }

                result = { 'id' : 'other-closed',
                           'license_url' : None,
                           'other' : None }

                self.assertEquals(translate_license_data(gemini), result)


                gemini = { 'use-limitations' : [ 'Nur im Ma&szlig;stab 1:50.000'  ],
                           'use-constraints' : [],
                           'access-constraints' : [],
                           'other-constraints' : [] }

                result = { 'id' : 'other-closed',
                           'license_url' : None,
                           'other' : 'Nutzungsbeschr&Auml;nkungen: Nur im Ma&szlig;stab 1:50.000' }

                self.assertEquals(translate_license_data(gemini), result)

                gemini = { 'use-limitations' : [ 'Nur im Ma&szlig;stab 1:50.000'  ],
                           'use-constraints' : [ 'copyright' ],
                           'access-constraints' : [],
                           'other-constraints' : [ 'http://example-license.com/' ] }

                result = { 'id' : 'other-closed',
                           'license_url' : 'http://example-license.com/', 
                           'other' : 'Nutzungsbeschr&Auml;nkungen: Nur ' +
                           'im Ma&szlig;stab 1:50.000 Urheberrecht ' +
                           'http://example-license.com/'}

                self.assertEquals(translate_license_data(gemini), result)

if __name__ == '__main__':
        unittest.main()
