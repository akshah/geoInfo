#!/usr/bin/python


#from __future__ import print_function
from contextlib import closing

import threading
import configparser
import re

import getopt
import time
import pymysql
import sys
import traceback
from customUtilities.helperFunctions import *
from customUtilities.logger import logger

def dbpush_asn_geo(db,asnF,location):
    #print("In dbpush_asn_geo")
    with closing( db.cursor()) as cur:
        try:
            cur.execute("select ASNLocation from ASNGeo where ASN = '{0}' and GeoDate='{1}'".format(asnF,geoDate))
            row=cur.fetchone()
            if row is not None: #We have seen this ASN-GeoDate before
                #countries=re.sub('[{}]','',row[0])
                #tmpSet=countries.split(',')
                #for entry in tmpSet:
                #   et=re.sub('[\"|\'| ]','',entry)
                #   location.add(et)
                print(asnF,str(location),flush=True)
                countries=eval(row[0])
                print(str(countries),flush=True)
                for ct in countries:
                    location.add(ct)
                print(str(location),flush=True)
                cur.execute('update ASNGeo set ASNLocation = "{0}" where ASN="{1}" and GeoDate = "{2}"'.format(str(location),asnF,geoDate))
            else:
                cur.execute("insert into ASNGeo(GeoDate,ASN,ASNLocation) values (%s,%s,%s)",(geoDate,asnF,str(location)))
            db.commit()
        except:
            traceback.print_exc()
            raise Exception('Insert to ASNGeo Failed')

def runAnalysis(db):
    #Read the file that contains updates to geolocation
    #Format expected:
    #IP|{'US','IN'..}|{AS1,AS2,..}  #AS set because IP can be announced by more than one AS
    logger.info('Preparing data to push.')
    with closing(open(updateFile,'r')) as fp:
        for lineRaw in fp:
            line=lineRaw.rstrip('\n')
            vals=line.split('|')
            #ip=vals[0]
            countrySet=eval(vals[1])
            asSet=eval(vals[2])
            for asn in asSet:
                if asn not in asCountryDict.keys():
                    asCountryDict[asn]=set()
                for country in countrySet:
                    asCountryDict[asn].add(country)
    logger.info('Starting data insert to DB.')
    logger.info(str(len(asCountryDict.keys()))+' ASNs to be updated.')
    for asnL,countrySetL in asCountryDict.items():
        print('To push: '+str(asnL)+' '+str(countrySetL),flush=True)
        if isTest:
            exit(0)
        dbpush_asn_geo(db,asnL,countrySetL)


if __name__ == "__main__":
    start_time,_=currentTime()

    if sys.version_info < (3,0):
        print("ERROR: Please use python3.",flush=True)
        exit(0)

    isTest=False

    updateFile='AdvancedIPCountryASPP.txt'
    asCountryDict={}

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
    logger=logger(logfilename)

    print('Start',flush=True)
    runAnalysis(db)
    print('End',flush=True)
    db.close()

    end_time,_=currentTime()
    logger.info('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
