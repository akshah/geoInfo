import subprocess
from geoInfo.ASNtoCountryRepo import ASNtoCountryRepo

def getCountry(AS):
    AS='AS'+str(AS)
    org="NA"
    if AS not in resolvedASes.keys():
        lines = subprocess.check_output(['whois', '-h', 'whois.cymru.com',AS],universal_newlines=True)
        output=lines.split("\n")
        vals=output[1].split(' ')
        country=vals[len(vals)-1]
        resolvedASes[AS]=country
    else:
        country=resolvedASes[AS]

    return country


if __name__=="__main__":
    resolvedASes={}

    asnRepo=ASNtoCountryRepo()
    asnRepo.load()

    for ASN in asnRepo.asHash.keys():
        whoISCountry=getCountry(ASN)
        if whoISCountry != 'ZZ' and whoISCountry != '' and whoISCountry != 'NO_NAME':
            if not asnRepo.asnIn(ASN,whoISCountry):
                print(ASN, whoISCountry, asnRepo.asHash[ASN])