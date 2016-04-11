#!/usr/bin/python


from __future__ import print_function
from contextlib import closing

import threading
import configparser
import re

import getopt
import time
import pymysql
import sys

def usage(msg="Usage"):
    print(msg)
    print('python3 '+sys.argv[0]+' -d RIBS_LOCATION [-h]')
    sys.exit(2)

def current_time():
    return int(time.time()),time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())

class Logger():

    def __init__(self,logfilename):
        self.lock = threading.RLock()
        self.logfilename = logfilename
    
    def print_log(self,msg):
        self.lock.acquire()
        try:
            
            #Log file
            logfile = open(self.logfilename,'a+')

            _,localtime=current_time()
            time='['+localtime+']  '
            print(time,'INFO:',msg,file=logfile)
            logfile.close()

        finally:
            self.lock.release()


            
def dbpush_asn_geo(db,asn,location): 
    #print("In dbpush_asn_geo")
    with closing( db.cursor() ) as cur: 
        try:
            cur.execute("select ASNLocation from ASNGeo where ASN = '{0}' and GeoDate='{1}'".format(asn,geoDate))
            row=cur.fetchone()
            if row is not None: #We have seen this ASN-GeoDate before
                countries=re.sub('[{}]','',row[0])
                tmpSet=countries.split(',')
                for entry in tmpSet:
                   et=re.sub('[\"|\'| ]','',entry)
                   location.add(et)
                #print(str(location))
                cur.execute("update ASNGeo set ASNLocation = '{0}' where GeoDate = '{1}'".format(str(location),geoDate))
            else:
                cur.execute("insert into ASNGeo(GeoDate,ASN,ASNLocation) values (%s,%s,%s)",(geoDate,asn,str(location)))
        except:
           raise Exception('Insert to ASNGeo Failed')

def query_get_distinct_asn(db):
    with closing( db.cursor() ) as cur:
        toReturn=[]
        try:
            cur.execute("SELECT distinct OriginAS FROM BGPPrefixGeo where GeoDate= '{0}'".format(geoDate))
            row=cur.fetchone()
            while row is not None:
                if '}' not in row[0] and '{' not in row[0]: #We will ignore Group of Origin ASes 
                    toReturn.extend(row)
                row=cur.fetchone()
     
        except Exception:
           raise Exception('Select Query Distinct ASN Failed')

        if toReturn:
            return toReturn
        else:
            return False


def query_asn_locations(db,asn):
    with closing( db.cursor() ) as cur:
        toReturn=set()
        try:
            cur.execute('SELECT distinct PrefixLocation FROM BGPPrefixGeo WHERE OriginAS = "{0}" and GeoDate="{1}"'.format(asn,geoDate))
            row=cur.fetchone()
            while row is not None:
                countries=re.sub('[{}]','',row[0])
                tmpSet=countries.split(',')
                #print("Row: "+row[0])
                #print("TMP: "+str(tmpSet))
                for entry in tmpSet:
                    if entry != "set()":
                        et=re.sub('[\"|\'| ]','',entry)
                        #print(entry,et)
                        toReturn.add(et)
                #print("Return: "+str(toReturn))
                row=cur.fetchone()
     
        except Exception:
           raise Exception('Select Query Failed')

        #Lookup presence in IXPs
        ixpDict=getIXPList(asn)
        ixpCountrySet=getCountriesFromIXPDict(ixpDict)
        if len(ixpCountrySet)>0:
            for ct in ixpCountrySet:
                if ct not in toReturn:
                    toReturn.add(ct)
                    with closing(open('countriesAddedFromIXPData.txt','a+')) as asncountryFile:
                        print(asn+"|"+ct,file=asncountryFile)

        if toReturn:
            return toReturn
        else:
            return "set()"
     
def getProcessedASN():
    try:
        f = open(list_of_already_processed_ASN)
    except:
        f = open(list_of_already_processed_ASN,'a+')
        logger.print_log(list_of_already_processed_ASN+' file created')
    with open(list_of_already_processed_ASN) as f:
            lines = f.read().splitlines()
    return lines
          
def print_to_processed_list(ASN):
    lock = threading.RLock()
    lock.acquire()
    try:
        f = open(list_of_already_processed_ASN,'a+')
        print(ASN,file=f)
        #Also update the in-memory list of processed prefixes
        processedASN.add(ASN)
        f.close()
    finally:
        lock.release()

def getIXPList(AS):
    ixpDict={}
    db = pymysql.connect(host=config['IXPMySQL']['serverIP'],
                         port=int(config['IXPMySQL']['serverPort']),
                         user=config['IXPMySQL']['user'],
                         passwd=config['IXPMySQL']['password'],
                         db=config['IXPMySQL']['dbname'])
    with closing(db.cursor()) as cur:
        try:
            query = "SELECT p.ID,ASn,ShortName,Name,City,Country,Continent FROM participants p ,ixps i where  p.ID=i.ID and ASn = '{0}'".format(AS)
            cur.execute(query)
            row = cur.fetchone()
            while row is not None:
                (ixpid,asn,shortName,name,city,country,continent)=row
                ixpDict[ixpid]={}
                ixpDict[ixpid]['asn']=asn
                ixpDict[ixpid]['shortName']=shortName
                ixpDict[ixpid]['name']=name
                ixpDict[ixpid]['city']=city
                ixpDict[ixpid]['country']=country
                ixpDict[ixpid]['continent']=continent
                row = cur.fetchone()
        except:
            logger.error('IXP fetch failed!')
    db.close()
    return ixpDict

def getCountriesFromIXPDict(ixpDict):
    countrySet=set()
    for ixpID in ixpDict.keys():
        countrySet.add(ixpDict[ixpID]['country'])
    return countrySet

        
def runAnalysis():
    ASN_List=query_get_distinct_asn(db)
    for ASN in ASN_List:
        if ASN not in processedASN:
            thisASLoc=set()
            thisASLoc=query_asn_locations(db,ASN)
            #print(ASN,str(thisASLoc))
            #if thisASLoc is False:
            #    print_to_processed_list(ASN)
            #else:
            dbpush_asn_geo(db,ASN,thisASLoc)
            print_to_processed_list(ASN)
            db.commit()
            
        
if __name__ == "__main__":
    start_time,_=current_time()
    
    if sys.version_info < (3,0):
        print("ERROR: Please use python3.")
        exit(0)
       
    isTest=False

    geoDate="20160105"

    config = configparser.ConfigParser()
    config.read('./conf/mrt2db_geo.conf')
    config.sections()

    db = pymysql.connect(host=config['MySQL']['serverIP'],
                         port=int(config['MySQL']['serverPort']),
                         user=config['MySQL']['user'],
                         passwd=config['MySQL']['password'],
                         db=config['MySQL']['dbname'])
    
    logfilename=None

    try:
        opts,args = getopt.getopt(sys.argv[1:],'l:h',['logfile','help'])
    except getopt.GetoptError:
        usage('GetoptError: Arguments not correct') 

    for opt,arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(2)
        elif opt in ('-l', '--logfile'):
            logfilename = arg 
    
    #Logger
    if not logfilename:
        scriptname=sys.argv[0].split('.')
        logfilename=scriptname[0]+'.log'
    logger=Logger(logfilename)
    
    list_of_already_processed_ASN="geo_processed_ASN.txt"
    processedASN = set(getProcessedASN())

    runAnalysis()
 
    db.close()

    end_time,_=current_time()
    logger.print_log('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')


