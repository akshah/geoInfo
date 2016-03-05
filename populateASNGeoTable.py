#!/usr/bin/python


from __future__ import print_function
from collections import defaultdict
from contextlib import closing
from multiprocessing import Pool,Process

from ASPaths.ASPath import ASPath
from ASPaths.DeepPathAnalysis import DeepPathAnalysis

from ThreadPool.TPool import TPool
import threading
import ipaddress
import ast
import random
import re
import signal,time
import subprocess
import getopt
import time
import pymysql
import sys
import os
from os import listdir
from os.path import isfile, join

from Country_Info.MaxMindRepo import MaxMindRepo

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
            cur.execute("select ASNLocation from {0}.{1} where ASN = {2}".format(dbname,'ASN_Geo',asn))
            row=cur.fetchone()
            if row is not None: #We have seen this ASN before 
                countries=re.sub('[{}]','',row[0])
                tmpSet=countries.split(',')
                for entry in tmpSet:
                   et=re.sub('[\"|\'| ]','',entry)
                   location.add(et)
            print(str(location))
            cur.execute("insert into ASN_Geo(ASN,ASNLocation) values (%s,%s)",(asn,str(location)))    
        except:
           raise Exception('Insert to ASN_Geo Failed')

def query_get_distinct_asn(db):
    with closing( db.cursor() ) as cur:
        toReturn=[]
        try:
            cur.execute("SELECT distinct OriginAS FROM {0}.{1}".format(dbname,'ASN_Prefix_Geo'))
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
            cur.execute('SELECT distinct PrefixLocation FROM {0}.{1} WHERE OriginAS = "{2}"'.format(dbname,'ASN_Prefix_Geo',asn))
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

        if toReturn:
            return toReturn
        else:
            return False

            
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
            if  numofhosts < 10:
                indices = random.sample(range(numofhosts),numofhosts)#Pick random IPs
            else:
                indices = random.sample(range(numofhosts),10)#Pick 10 random IPs at max
            
            for index in indices:
                allHostsSampled.append(allHosts[index])
                        
            #Getting the geolocation
            locations = set()
            for host in allHostsSampled:
                locations.update(maxmind.ipToCountry(str(host)))
                prefix_locations=prefix_locations.union(locations)
            #Write the following to FILE-A    
            tofileA=prefix+"\t"+str(net)+"\t"+str(locations)
            toPushA.append(tofileA)
        
        #Write the following to FILE-B
        tofileB=OriginAS+"\t"+prefix+"\t"+str(prefix_locations)
        toPushB.append(tofileB)
             
        writeObj.write_prefix_block_geo(toPushA)
        writeObj.write_asn_prefix_geo(toPushB)
     
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
        
def runAnalysis():
    ASN_List=query_get_distinct_asn(db)
    for ASN in ASN_List:
        if ASN not in processedASN:
            thisASLoc=set()
            thisASLoc=query_asn_locations(db,ASN)
            #print(ASN,str(thisASLoc))
            if thisASLoc is False:
                print_to_processed_list(ASN)
            else:
                dbpush_asn_geo(db,ASN,thisASLoc)
                print_to_processed_list(ASN)
                db.commit()
            
        
if __name__ == "__main__":
    start_time,_=current_time()
    
    if sys.version_info < (3,0):
        print("ERROR: Please use python3.")
        exit(0)
       
    isTest=False
       
    dbname="testdetoursdb"
    
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
    
    #Prepare DB info
    db = pymysql.connect(host="proton.netsec.colostate.edu",
                     user="root", 
                     passwd="n3ts3cm5q1", 
                    db=dbname)

    runAnalysis()
 
    db.close()

    end_time,_=current_time()
    logger.print_log('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')


