from collections import defaultdict
import pymysql
import re
from contextlib import closing

'''
Loads AS Geolocation in memory after reading from DB
'''
from multiprocessing import Lock
ASNtoCountryFile='CountryInfo/oldGeoFiles/AS_to_Geo.txt'
class ASNtoCountryRepo():
    
    def __init__(self):
        self.asHash = defaultdict(set) # dic from asn to set of conts
        #self.db=db
        #self.loadD()
    
    def load(self,dbname,db):
        '''This function will pull 48K entries from DB'''
        #print('Starting to load AS locations')
        with closing( db.cursor() ) as cur:
            toReturn=[]
            try:
                cur.execute('select ASN,ASNLocation from {0}.{1};'.format(dbname,'ASN_Geo'))
                row=cur.fetchone() 
                while row is not None:
                    asn = row[0]
                    countries=re.sub('[{}]','',row[1])
                    tmpSet=countries.split(',')
                    location=set()
                    for entry in tmpSet:
                        et=re.sub('[\"|\'| ]','',entry)
                        location.add(et)
                    #Sanitizing
                    if "***" in location:
                        location.remove("***")
                    if(len(location) == 0):
                        continue
                    
                    #Adding to the hash
                    self.asHash[asn].update(location)
                    #toReturn.append(row)
                    row=cur.fetchone()     
            except Exception:
               raise Exception('Fetch AS locations query failed')
    
            #if toReturn:
            #    return toReturn
            #else:
            #    return False
        #print('Finished loading AS locations')
          
    def getNumASNs(self):
        return len(self.asHash)
    
    def getCountries(self, asn):
        try: 
            return self.asHash[asn]
        except KeyError: 
            return set()
        
    def asnIn(self, asn, country):
        #return False if asn not in self.asHash or country not in self.asHash[asn] else True
        try:
            return country in self.asHash[asn]
        except KeyError:
            return False
    
