'''
Created on Jan 20, 2016

@author: akshah
'''

import MySQLdb as pymysql
from MySQLdb.connections import IntegrityError
import pycurl
import configparser
from contextlib import closing
import os
import time
import ipaddress
import netaddr
from urllib.request import urlopen
from customUtilities.logger import logger
import time
import zipfile
import gzip
import traceback

class maxMindDataEngine(object):
    '''
    Module to download MaxMind files and push to MySQL
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.logger=logger('maxMindDataEngine.log')
        self.data_date=self.timeStamp()[1]
        self.logger.info('MaxMind Data Engine initialized.')
        
    def timeStamp(self):
        return int(time.time()),time.strftime('%Y%m%d',time.localtime())
        
    def downloadFile(self,url):
            try:
                f = urlopen(url)
                fileName=os.path.basename(url)
                fileName=self.timeStamp()[1]+'_'+fileName
                # Open our local file for writing
                with open(fileName, "wb") as local_file:
                    local_file.write(f.read())
            except:
                self.logger.error("Download Error: "+url)

            print(fileName)
            return fileName
        
    def downloadMaxMindBinFile(self):
        fileName=self.downloadFile('http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz')
        return fileName

    def downloadMaxMindCsvFile(self):
        fileName=self.downloadFile('http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip')
        #zip_ref = zipfile.ZipFile(fileName, 'r')
        #zip_ref.extractall()
        #zip_ref.close()
        zipRef = zipfile.ZipFile(fileName)
        infoList=zipRef.namelist()
        self.data_date=infoList[0].split('/')[0].split('_')[1]
        zipRef.close()
        
        
        return fileName
        
    def downloadMaxMindFiles(self):
        filesDownloaded=[]
        try:
            binFile=self.downloadMaxMindBinFile()
            filesDownloaded.append(binFile)
            csvFile=self.downloadMaxMindCsvFile()
            filesDownloaded.append(csvFile)
        except:
            self.logger.error('Error in download.')
    
        return filesDownloaded
    
    def push2DB(self,data):
        try:
            config = configparser.ConfigParser()
            config.read('./conf/maxmindDataEngine.conf')
            config.sections()

            db = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])

            with closing( db.cursor() ) as cur: 
                try:
                    cur.execute("set net_read_timeout=300; set net_write_timeout=300;SET GLOBAL connect_timeout=28800;\
                    SET GLOBAL wait_timeout=28800;SET GLOBAL interactive_timeout=28800")
                    #db.commit()
                except:
                    traceback.print_exc()
                    self.logger.warn('Timeouts could not be updated')
                    print('Timeouts could not be updated')
            with closing( db.cursor() ) as cur: 
                try:
                    cur.execute("set net_read_timeout=300; set net_write_timeout=300;SET GLOBAL connect_timeout=28800;\
                    SET GLOBAL wait_timeout=28800;SET GLOBAL interactive_timeout=28800; insert ignore into archive_files(id,insert_time,data_date,gz_bin_file,zip_csv_file) values (%s,%s,%s,%s,%s)",data)
                    #cur.execute("select count(*) from archive_files;")
                    print('Insert of BLOB successful!')
                except IntegrityError:
                    self.logger.warn('Possibly duplicate files were tried to be inserted. Ignoring it.')
                    #return
                except:
                    traceback.print_exc()
            db.commit()
            db.close()
                    #raise Exception('Insert to archive_files Failed')
            self.logger.info('Push files to DB successfully.')
                    
        except:
            traceback.print_exc()
            self.logger.error('Error in Pushing to DB.')

    def fetchMaxMindFiles(self):
        self.logger.info('Fetching MaxMind files.')
        try:
            data=[]
            data.append(None)
            data.append(str(time.time()))
            fileList=self.downloadMaxMindFiles()
            data.append(self.data_date)
            with open(fileList[0], 'rb') as f:
                binFile = f.read()
            os.remove(fileList[0])
            data.append(binFile)
            with open(fileList[1], 'rb') as f:
                csvFile = f.read()
            os.remove(fileList[1])
            data.append(csvFile)    
            
            #print(data[0],data[1],data[2])
            self.push2DB(data)
            
        except:
            self.logger.error('Could not properly finish pushing maxmind files.')
            
    def getMaxMindArchiveBinFile(self,day='latest',location='.'):
        try:
            config = configparser.ConfigParser()
            config.read('./conf/maxmindDataEngine.conf')
            config.sections()

            db = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])

            with closing( db.cursor() ) as cur: 
                try:
                    if day == "latest":
                        cur.execute("select gz_bin_file from archive_files ORDER BY id DESC")
                    else:
                        cur.execute("select gz_bin_file from archive_files where data_date = {0}".format(day))
                    row=cur.fetchone()
                    fileName=location+'/'+day+'_maxmind_bin.gz'
                    with open(fileName, 'wb') as f:
                        f.write(row[0])
                        
                    inF = gzip.open(fileName, 'rb')
                    outF = open(fileName[:-3], 'wb')
                    outF.write( inF.read() )
                    inF.close()
                    outF.close()
                    os.remove(fileName)
                    self.logger.info('Downloaded from maxmind_archive: '+fileName[:-3])
                except:
                    raise Exception('Reading bin_archive_file Failed')
            db.close()  
        except:
            traceback.print_exc()
            self.logger.error('Error in reading from archive.')
            
    def maxMindArchiveDates(self):
        try:
            #Prepare DB info
            db = pymysql.connect(host="proton.netsec.colostate.edu",
                     user="root", 
                     passwd="****", 
                    db='maxmind_archive')
            dates=[]
            with closing( db.cursor() ) as cur: 
                try:
                    cur.execute("select data_date from archive_files order by id desc")
                    row=cur.fetchone()
                    while row:
                        dates.append(row[0])
                        row=cur.fetchone()

                except:
                    raise Exception('Reading dates from archive_files Failed')
            return dates
        
            db.close()
        except:
            traceback.print_exc()
            self.logger.error('Error in reading from archive.')
        
