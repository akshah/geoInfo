import pygeoip as pip

class MaxMindRepo(object):
    """Read MaxMind binary file to return IPv4 location"""
    instance = None
    
    def __init__(self,binFileLocation='/home3/akshah/akshah_cron_bin/latest_maxmind_bin'):
        self.binFileLocation=binFileLocation
        #TODO: If the above file does not exists then this module should use maxmindDataEngine and pull the latest bin
        self.contDB = pip.GeoIP(self.binFileLocation, pip.MEMORY_CACHE)

    def get(self,cls):
        cls.lock.acquire()
        if cls.instance is None:
            cls.instance = cls()
        cls.lock.release()
        return cls.instance        
  
    def ipToCountry(self, ip):
        try:
            toReturn = set()
            toReturn.add(self.contDB.record_by_addr(ip)['country_code'])
            return toReturn
        except TypeError:
            return set()
        
    def ipLocation(self, ip):
        try:
            gir=self.contDB.record_by_addr(ip)
            if gir is not None:
                return str(gir)
            
        except TypeError:
            return ""
    