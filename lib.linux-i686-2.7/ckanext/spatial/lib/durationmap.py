import json, urllib2
import re

class DurationTranslator:

    def translate_duration_data(self, duration, duration_factor):
        
        temp_duration = None
        temp_factor = None
        
        if duration == 'forthnightly':
                    temp_duration = 'tag'
                    temp_factor = 14                    
        elif duration == 'daily':  
                    temp_duration = 'tag'
                    temp_factor = 1             
        elif duration == 'weekly':  
                    temp_duration = 'woche'
                    temp_factor = 1
        elif duration == 'biannually':
                    temp_duration = 'monat'
                    temp_factor = 6 
        elif duration == 'monthly':  
                    temp_duration = 'monat'
                    temp_factor = 1
        elif duration == 'quarterly':  
                    temp_duration = 'monat'
                    temp_factor = 3
        elif duration == 'annually':     
                    temp_duration = 'jahr'
                    temp_factor = 1        
           
        result = self.translate_duration_factor(duration_factor)
        
        
        if temp_duration == result['duration']:
            temp_factor =  result['duration_factor']  
        elif result['duration'] and result['duration_factor'] :  
            temp_duration = result['duration']
            temp_factor =  result['duration_factor']  
            
        result = {'duration' :'', 'duration_factor' : '' } 
        result['duration'] = temp_duration
        result['duration_factor'] = temp_factor
        
        return result
                    
    
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
                                    duration = 'sekunde'
                                    temp_factor = duration_factors['seconds']  
                            
        result['duration'] = duration
        result['duration_factor'] = temp_factor
        return result













