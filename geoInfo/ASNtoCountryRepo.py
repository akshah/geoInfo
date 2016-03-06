import re
from collections import defaultdict
from contextlib import closing

'''
Loads AS Geolocation in memory after reading from MySQL DB
'''

class ASNtoCountryRepo():

    def __init__(self):
        self.asHash = defaultdict(set)

    def load(self,dbname,db):
        #This function will pull about 48K entries from DB
        #print('Starting to load AS locations')
        with closing( db.cursor() ) as cur:
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

    def getNumASNs(self):
        return len(self.asHash)

    def getCountries(self, asn):
        try:
            return self.asHash[asn]
        except KeyError:
            return set()

    def asnIn(self, asn, country):
        #
        # Return False if asn not in self.asHash or country not in self.asHash[asn] else True
        try:
            return country in self.asHash[asn]
        except KeyError:
            return False

