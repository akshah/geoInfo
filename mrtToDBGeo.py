#!/usr/bin/python


from __future__ import print_function
from collections import defaultdict
from contextlib import closing
import configparser
from multiprocessing import Pool,Process
from customUtilities.processPool import processPool
import threading
import ipaddress
import ast
import random
import signal,time
import subprocess
import getopt
import time
import pymysql
import sys
import os
from os import listdir
from os.path import isfile, join

from geoInfo.MaxMindRepo import MaxMindRepo

def usage(msg="Usage"):
    print(msg)
    print('python3 '+sys.argv[0]+' -d RIBS_LOCATION [-h]')
    sys.exit(2)

def current_time():
    return int(time.time()),time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())

class writeToDisk():

    def __init__(self):
        self.lock = threading.RLock()
                
    def write_prefix_block_geo(self,valList):
        self.lock.acquire()
        try:
            Lfile = open(prefix_block_geo,'a+')
            for val in valList:
                print(val,file=Lfile)
            Lfile.close()
        finally:
            self.lock.release()
    
    def write_asn_prefix_geo(self,valList):
        self.lock.acquire()
        try:
            Lfile = open(asn_prefix_geo,'a+')
            for val in valList:
                print(val,file=Lfile)
            Lfile.close()
        finally:
            self.lock.release()

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

def print_list_of_processed_ribs(fullribname):
    lock = threading.RLock()
    lock.acquire()
    try:
        Lfile = open(list_of_already_processed_ribs,'a+')
        ribname=fullribname.split("/")
        print(ribname[len(ribname)-1],file=Lfile)
        Lfile.close()
    finally:
        lock.release()

            
def getProcessedRibs():
    try:
        f = open(list_of_already_processed_ribs)
    except:
        f = open(list_of_already_processed_ribs,'a+')
        logger.print_log(list_of_already_processed_ribs+' file created')
    with open(list_of_already_processed_ribs) as f:
            lines = f.read().splitlines()
    return lines

def print_list_of_processed_prefixes(asnprefix):
    lock = threading.RLock()
    lock.acquire()
    try:
        Lfile = open(list_of_already_processed_prefixes,'a+')
        print(asnprefix,file=Lfile)
        #Also update the in-memory list of processed prefixes
        processedPrefixes.add(asnprefix)
        Lfile.close()
    finally:
        lock.release()

def print_unresolved_ip(prefixHost):
    lock = threading.RLock()
    lock.acquire()
    try:
        Lfile = open(unresolved_ips,'a+')
        print(prefixHost,file=Lfile)
        Lfile.close()
    finally:
        lock.release()

def print_unused_asprefix(asprefix):
    lock = threading.RLock()
    lock.acquire()
    try:
        Lfile = open(unused_asprefix,'a+')
        print(asprefix,file=Lfile)
        Lfile.close()
    finally:
        lock.release()


def getProcessedPrefixes():
    try:
        f = open(list_of_already_processed_prefixes)
    except:
        f = open(list_of_already_processed_prefixes,'a+')
        logger.print_log(list_of_already_processed_prefixes+' file created')
    with open(list_of_already_processed_prefixes) as f:
            lines = f.read().splitlines()
    return lines

def dbpush_prefix_block_geo(db):
    data=[]
    with closing( db.cursor() ) as cur:
        try:
            fn=open(prefix_block_geo)
            lines=fn.read().splitlines()
            for line in lines:
                vals=line.split("\t")
                if len(vals) != 3:
                    continue
                tmp=[]
                tmp.append(geoDate)
                tmp.append(vals[0])
                tmp.append(vals[1])
                tmp.append(vals[2])
                data.append(tmp)
            fn.close()
            cur.executemany("insert into BlockGeo(GeoDate,BGPPrefix,Sub24Block,BlockLocation) values (%s,%s,%s,%s)",data)
            db.commit()

        except:
           raise Exception('Insert to BlockGeo Failed')

def dbpush_asn_prefix_geo(db):
    data=[]
    with closing( db.cursor() ) as cur:
        try:
            fn=open(asn_prefix_geo)
            lines=fn.read().splitlines()
            for line in lines:
                vals=line.split("\t")
                if len(vals) != 3:
                    continue
                tmp=[]
                tmp.append(geoDate)
                tmp.append(vals[0])
                tmp.append(vals[1])
                tmp.append(vals[2])
                data.append(tmp)
            fn.close()
            cur.executemany("insert into BGPPrefixGeo(GeoDate,OriginAS,BGPPrefix,PrefixLocation) values (%s,%s,%s,%s)",data)
            db.commit()
        except:
           raise Exception('Insert to BGPPrefixGeo Failed')


def deleteContent(fName):
    with open(fName, "w"):
        pass


def processEachPrefix(asnprefix):
        vals=asnprefix.split('|')
        OriginAS=vals[0]
        prefix=vals[1]
        toPushA =[]
        toPushB =[]
        NULL=None
        #Get all /24s for the Prefix
        if prefix == '0.0.0.0/0':
            return

        network = ipaddress.IPv4Network(prefix)

        all24 = [network] if network.prefixlen >= 24 else network.subnets(new_prefix=24)
        prefix_locations = set()
        for net in all24:
            #print(str(net))
            allHosts = list(net.hosts())
            indices=[]
            allHostsSampled=[]
            numofhosts=len(allHosts)
            #if  numofhosts < 10:
            #indices = random.sample(range(numofhosts),numofhosts)#Pick all IPs
            #else:
                #indices = random.sample(range(numofhosts),10)#Pick 10 random IPs at max

            for index in range(0,numofhosts):
                allHostsSampled.append(allHosts[index])

            #Getting the geolocation
            locations = set()
            for host in allHostsSampled:
                locations.update(maxmind.ipToCountry(str(host)))
                if len(locations) == 0:
                    print_unresolved_ip(prefix+'|'+str(host))
                prefix_locations=prefix_locations.union(locations)
            #Write the following to FILE-A
            tofileA=prefix+"\t"+str(net)+"\t"+str(locations)
            toPushA.append(tofileA)

        #Write the following to FILE-B
        tofileB=OriginAS+"\t"+prefix+"\t"+str(prefix_locations)
        toPushB.append(tofileB)

        writeObj.write_prefix_block_geo(toPushA)
        writeObj.write_asn_prefix_geo(toPushB)


def runAnalysis(onlyfiles):
    numfile=0
    totalfiles=len(onlyfiles)
    asnPrefixDict={}
    toProcessSet =set()
    for fn in onlyfiles:
        numfile+=1

        #Check if this file was not processed before
        processedRibs = getProcessedRibs()

        deleteContent(prefix_block_geo)
        deleteContent(asn_prefix_geo)

        ribname=fn.split("/")
        fnName=ribname[len(ribname)-1]

        if fnName in processedRibs:
            logger.print_log('MRT file '+fn+' was previously processed. Skipping it.')
            continue
        elements=fn.split('.')
        dataDay=elements[len(elements)-3]
       #filename=dirpath+'/'+fn
        filename=fn
        logger.print_log("bgpdump on " + filename)
        bashCommand='bgpdump -m '+filename

        lines=[]
        try:
            lines = subprocess.check_output(["bgpdump", "-m", filename],universal_newlines=True)
        except:
            continue


        logger.print_log('Creating ASN-Prefix list for '+filename)
        for line in lines.split("\n"):
            if not line.startswith("TAB"):
                continue
            pieces = line.split('|')

            prefix = pieces[5]
            prefix.rstrip()
            #Validate IPv4 Prefix
            try:
                net = ipaddress.IPv4Network(prefix)
            except:
                continue #Not a valid v4 address,move on

            aspath = pieces[6]
            aspath.rstrip()
            all_ASes_orig=aspath.split(' ')
            OriginAS=all_ASes_orig[len(all_ASes_orig)-1]
            if OriginAS not in asnPrefixDict.keys():
                asnPrefixDict[OriginAS]={}
            if prefix not in asnPrefixDict[OriginAS].keys():
                asnPrefixDict[OriginAS][prefix]=[]
            if dataDay not in asnPrefixDict[OriginAS][prefix]:
                asnPrefixDict[OriginAS][prefix].append(int(dataDay))

        print_list_of_processed_ribs(filename)
        logger.print_log('Done processing for '+filename)

    for oas in asnPrefixDict.keys():
        for prf in asnPrefixDict[oas].keys():
            keyOriginASprefix=oas+"|"+prf
            sortedDayList=sorted(asnPrefixDict[oas][prf])
            valCounter=0
            validOrigin=False
            for i in range(0,len(sortedDayList)-1):
                if(sortedDayList[i+1]==sortedDayList[i]+1):
                    valCounter+=1
                    if valCounter >= 7:
                        validOrigin=True
                        break
                else:
                    valCounter=0

            if validOrigin:
                if keyOriginASprefix in processedPrefixes:
                    continue #Skip this originasn-prefix
                else:
                    toProcessSet.add(keyOriginASprefix)
            else:
                print_unused_asprefix(keyOriginASprefix)

    toProcess=list(toProcessSet)   #Remove Duplicate Prefixes
    #logger.print_log('List created. '+str(len(toProcess))+' new prefixes to be processed for '+filename)
    numTh=25
    inner_pool=processPool(numThreads=numTh)
    if isTest:
        toProcess=toProcess[:10000]
    retvals=inner_pool.runParallelWithPool(processEachPrefix,toProcess)
    dbpush_prefix_block_geo(db)
    dbpush_asn_prefix_geo(db)
    db.commit()

    for entry in toProcess:
        print_list_of_processed_prefixes(entry)
    logger.print_log('Done all processing')

if __name__ == "__main__":
    start_time,_=current_time()

    if sys.version_info < (3,0):
        print("ERROR: Please use python3.")
        exit(0)

    isTest=False

    #dbname="geoinfo_archive"
    list_of_already_processed_ribs="geo_processed_ribs.txt"
    list_of_already_processed_prefixes="geo_processed_prefixes.txt"
    prefix_block_geo="prefix_block_geo.txt"
    f=open(prefix_block_geo,'w')
    f.close()
    asn_prefix_geo="asn_prefix_geo.txt"
    f=open(asn_prefix_geo,'w')
    f.close()
    unresolved_ips='unresolved_ips.txt'
    f=open(unresolved_ips,'w')
    f.close()
    unused_asprefix='unused_asprefix.txt'
    f=open(unused_asprefix,'w')
    f.close()
    maxmind = MaxMindRepo('/home3/akshah/maxmindFiles/20160105_maxmind_bin')
    geoDate='20160105'
    
    logfilename=None
    dirpath=None
    try:
        opts,args = getopt.getopt(sys.argv[1:],'l:d:h',['logfile','directory','help'])
    except getopt.GetoptError:
        usage('GetoptError: Arguments not correct') 

    for opt,arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(2)
        elif opt in ('-l', '--logfile'):
            logfilename = arg 
        elif opt in ('-d', '--directory'):
            dirpath = arg

    if not dirpath:
        usage('Missing directory to read ASN|Prefix from')

    #Logger
    if not logfilename:
        scriptname=sys.argv[0].split('.')
        logfilename=scriptname[0]+'.log'
    logger=Logger(logfilename)
    writeObj=writeToDisk()
    #Get list prefixes that were processed before, if any
    processedPrefixes = set(getProcessedPrefixes())
    
    #Read files in the directory given (-d option)
    #onlyfiles = [ f for f in listdir(dirpath) if isfile(join(dirpath,f)) ]
    mrtfiles=[]
    for dp, dn, files in os.walk(dirpath):
        for name in files:
            if name.lower().endswith('.gz') or name.lower().endswith('.bz2') or name.lower().endswith('.mrt'):
                mrtfiles.append(os.path.join(dp, name))

    config = configparser.ConfigParser()
    config.read('./conf/mrt2db_geo.conf')
    config.sections()

    db = pymysql.connect(host=config['MySQL']['serverIP'],
                          port=int(config['MySQL']['serverPort']),
                          user=config['MySQL']['user'],
                          passwd=config['MySQL']['password'],
                          db=config['MySQL']['dbname'])


    mrtfiles.sort()
    #print(mrtfiles)
    runAnalysis(mrtfiles)


    end_time,_=current_time()
    logger.print_log('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')


