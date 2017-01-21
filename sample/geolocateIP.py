from __future__ import print_function
from geoInfo.MaxMindRepo import MaxMindRepo
import sys
import traceback
#if len(sys.argv) != 2:
#   print('Give me an IP to geolocate.')


maxmind = MaxMindRepo('/home3/akshah/akshah_cron_bin/latest_maxmind_bin')
#maxmind = MaxMindRepo()
print('Using bin file: '+maxmind.binFileLocation)
print('host|country_code|region_code|city|latitude|longitude')
for lR in sys.stdin:
    try:
        line=lR.rstrip('\n').rstrip('\r')
        host=line
        #print(maxmind.ipToCountry(str(host)))
        dict=eval(maxmind.ipLocation(str(host)))
        #for key in dict:
        #    print(key,': ',dict[key])
        print(str(host)+'|'+str(dict['country_code'])+'|'+str(dict['region_code'])+'|'+ \
              str(dict['city'])+'|'+str(dict['latitude'])+'|'+str(dict['longitude']))
    except:
        print(str(host))
        traceback.print_exc()