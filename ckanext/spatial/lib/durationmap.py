import json, urllib2
import re

class DurationTranslator:

    def translate_duration_data(self, duration):
        
        map = { 'daily' : 'tag',
                'weekly' : 'woche',
                'monthly' : 'monat',
                'quarterly' : 'quartal',
                'annually' : 'jahr',
                'continual' : None,
                'forthnightly' : None,
                'biannually' : None,
                'asNeeded' : None,
                'irregular' : None,
                'notPlanned' : None,
                'unknown' :None
              }
  
        out = None
        if duration in map.keys():
            out = map[duration]
        return out
    
    def translate_duration_factor(self, period):
        
        regex = re.compile('P(?:(?P<years>\d+)Y)?(?:(?P<months>\d+)M)?(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?')
        duration_factors = None
        # Fetch the match groups with default value of 0 (not None)
        match = regex.match(period)
        
        if match:
            duration_factors = match.groupdict(0)
            
        result = {'duration' :'', 'duration_factor' : '' }   
        duration = ''
        temp_factor = ''
             
        if duration_factors:
            if duration_factors['years'] > 0:
                if duration_factors['years'] == 5:
                    duration = '5-jahre'
                    temp_factor = 0
                else:
                    duration = 'jahr'
                    temp_factor = duration_factors['years']  
            else:            
                if duration_factors['months'] > 0:
                    duration = 'monat'
                    temp_factor = duration_factors['months']  
                else:            
                    if duration_factors['days'] > 0:
                        duration = 'tag'
                        temp_factor = duration_factors['days']  
                    else:            
                        if  duration_factors['hours'] > 0:
                            duration = 'stunde'
                            temp_factor = duration_factors['hours']                            
                        else:                  
                            if duration_factors['minutes'] > 0:
                                duration = 'minute'
                                temp_factor = duration_factors['minutes']                                  
                            else:
                                if duration_factors['days'] > 0:
                                    duration = 'mekunde'
                                    temp_factor = duration_factors['seconds']  
                            
        result['duration'] = duration
        result['duration_factor'] = temp_factor
        return result
        
        
    


import unittest
class TestMapping(unittest.TestCase):

    
    def test_e2g(self):
        u = DurationTranslator()
        translated = u.translate_duration_data('weekly')
        print translated
        self.assertEqual(translated, 'woche')

    def test_missing(self):
        u = DurationTranslator()
        translated = u.translate_duration_data(['biannually'])
        print translated
        self.assertEqual(translated, None)


if __name__ == '__main__':
    unittest.main()
    
    












