from __future__ import print_function
from geoInfo.MaxMindRepo import MaxMindRepo
import sys

if len(sys.argv) != 2:
    print('Give me an IP to geolocate.')
host=sys.argv[1]

maxmind = MaxMindRepo('/home3/akshah/akshah_cron_bin/latest_maxmind_bin')
#maxmind = MaxMindRepo()
print('Using bin file: '+maxmind.binFileLocation)
#print(maxmind.ipToCountry(str(host)))
dict=eval(maxmind.ipLocation(str(host)))
for key in dict:
    print(key,': ',dict[key])