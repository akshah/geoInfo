'''
Created on Jan 20, 2016

@author: akshah
'''
from geoInfo.maxMindEngine import maxMindEngine
import time


def current_time():
    return int(time.time()),time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())

if __name__ == '__main__':
    start_time,_=current_time()
    
    mde=maxMindEngine()
    
    #Download and push to DB from MaxMind
    #mde.fetchMaxMindFiles()
    
    #Check all dates available
    #dates=mde.maxMindArchiveDates()    
    #for date in dates:
    #    print(date)
    #mde.getMaxMindArchiveBinFile(day='20160105',location='/raid/maxmind_archive/')
    
    #Get latest
    #mde.getMaxMindArchiveBinFile(location='/raid1/akshah/maxmind_archive')
    
    #Get latest and save to current directory itself
    mde.getMaxMindArchiveBinFile()

    #Get latest and save to current directory itself
    mde.getMaxMindArchiveBinFile(day='20160105')
    
    end_time,_=current_time()
    print('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
