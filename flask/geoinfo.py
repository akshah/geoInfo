#!/usr/bin/env python

import sys, time
from flask import Flask,redirect
from flask import g
from flask import Response, render_template
from flask import request
from flask_limiter import Limiter 
import json
import MySQLdb
import ipaddress


app = Flask(__name__)

limiter = Limiter(app, global_limits=["60 per second","1000 per minute"])
#app.port=8080
#app.threaded=True
#app.debug=True

##ERROR MESSAGES
ERR_101={"error_code": 101,"error_message": "User input error. Incorrect data date. Use year \
and month e.g., 201601. See http://geoinfo.bgpmon.io/data_dates for all available data dates or \
http://geoinfo.bgpmon.io/latest_data_date to get the most recent date."}
ERR_102={"error_code": 102,"error_message": "User input error. No or incorrect query specified. \
See http://geoinfo.bgpmon.io/readme for supported queries."}
ERR_103={"error_code": 103,"error_message": "User input error. No or incorrect ASN specified. \
Example: http://geoinfo.bgpmon.io/201601/asn_country/12145"}
ERR_104={"error_code": 104,"error_message": "User input error. No or incorrect BGP prefix specified. \
Example: http://geoinfo.bgpmon.io/201601/bgp_prefix_country/129.82.0.0/16"}
ERR_105={"error_code": 105,"error_message": "User input error. No or incorrect ASN specified. \
Example: http://geoinfo.bgpmon.io/201601/prefixes_announced_from_asn/12145"}
ERR_106={"error_code": 106,"error_message": "User input error. No or incorrect country specified. \
Example: http://geoinfo.bgpmon.io/201601/prefixes_announced_from_country/KE"}
ERR_107={"error_code": 107,"error_message": "User input error. No or incorrect number specified. \
Example: http://geoinfo.bgpmon.io/201601/bgp_prefixes_num_countries/=2"}
ERR_108={"error_code": 107,"error_message": "User input error. No or incorrect number specified. \
Example: http://geoinfo.bgpmon.io/201601/slash24_prefixes_num_countries/=2"}

@app.before_request
def db_connect():
    g.conn = MySQLdb.connect(host='proton.netsec.colostate.edu',
			 user='netsecstudent',
			 passwd='n3ts3cL@bs',
			 db='geoinfo_archive')
    g.cursor = g.conn.cursor()

@app.after_request
def db_disconnect(response):
    g.cursor.close()
    g.conn.close()
    return response

def query_db(query, args=(), one=False):
    g.cursor.execute(query, args)
    rv = [dict((g.cursor.description[idx][0], value)
    for idx, value in enumerate(row)) for row in g.cursor.fetchall()]
    return (rv[0] if rv else None) if one else rv

def check(gedate):
    if not len(gedate)==6:
        return False
    try:
        intdate=int(gedate)
        return True
    except:
        return False

def checkint(val):
    try:
        intdate=int(val)
        if intdate>0:
            return True
        else:
            return False
    except:
        return False

def check_prefix(ip,leng):
    try:
        net1 = ipaddress.IPv4Address(ip)
        if int(leng)>=0 and int(leng)<=32:
            return True
    except:
        return False


@app.route("/")
def hello():
    #return "You can use this service to query BGP data characteristics.  See http://bgpmon.io/geoinfo/readme"

    readme=''
    fp=open('/home3/akshah/public_html/apireadme.html','r')	
    for line in fp:
	line=line.rstrip()
	readme=readme+line
    fp.close()	
    
    return readme

@app.route("/readme", methods=['GET'])
def readme():
    readme=''
    fp=open('/home3/akshah/public_html/apireadme.html','r')	
    for line in fp:
	line=line.rstrip()
	readme=readme+line
    fp.close()	
    
    return readme

@app.route("/feedback", methods=['GET'])
def feedback():
    #feedback=''
    #fp=open('/home3/akshah/public_html/feedback.html','r')	
    #for line in fp:
	#line=line.rstrip()
	#feedback=feedback+line
    #fp.close()	
    #return feedback
    return redirect("http://alpha.netsec.colostate.edu/~akshah/feedback.html")
    
 
@app.route("/sample", methods=['GET'])
def sample():
    readme=''
    fp=open('/home3/akshah/public_html/sampleapicode.html','r')	
    for line in fp: 
	readme=readme+line
    fp.close()	
    
    return readme

@app.route('/data_dates', methods=['GET'])
def data_dates():
    result = query_db("select distinct GeoDate from ASNGeo")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp

@app.route('/latest_data_date', methods=['GET'])
def latest_data_date():
    result = query_db("select max(distinct GeoDate) as 'GeoDate' from ASNGeo")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp

@app.route("/<geodate>", methods=['GET'])
def only_geodate(geodate):
    if not check(geodate):
        err=str([ERR_101,ERR_102])
        resp = Response(err, status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_102), status=400, mimetype='application/json')
    return resp

@app.route("/<geodate>/", methods=['GET'])
def only_geodate_with_slash(geodate):
    if not check(geodate):
        err=str([ERR_101,ERR_102])
        resp = Response(err, status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_102), status=400, mimetype='application/json')
    return resp

#@app.route("/get_AS_country/<asn>", methods=['GET'])
#def get_AS_country(asn):
 #   return "Deprecated. Use asn_country instead. \nExample: http://marshal.netsec.colostate.edu:5000/asn_country/299"

@app.route("/<geodate>/asn_country/", methods=['GET'])
def Oasn_country(geodate):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_103), status=400, mimetype='application/json')
    return resp
@app.route('/<geodate>/asn_country/<asn>', methods=['GET'])
def asn_country(geodate,asn):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    if not checkint(asn):
        resp = Response(str(ERR_103), status=400, mimetype='application/json')
        return resp
    result = query_db("select ASNLocation,ASN from ASNGeo where ASN = "+asn+" and GeoDate like \'"+geodate+"%%\'")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp

@app.route("/<geodate>/bgp_prefix_country/", methods=['GET'])
def Obgp_prefix_country(geodate):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_104), status=400, mimetype='application/json')
    return resp
@app.route('/<geodate>/bgp_prefix_country/<bgpprefix>', methods=['GET'])
def bgp_incorrect_prefix_country(geodate,bgpprefix):
    if not check(geodate):
        resp = Response(str([ERR_101,ERR_104]), status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_104), status=400, mimetype='application/json')
    return resp
@app.route('/<geodate>/bgp_prefix_country/<bgpprefix>/<leng>', methods=['GET'])
def bgp_prefix_country(geodate,bgpprefix,leng):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    if not check_prefix(bgpprefix,leng):
        resp = Response(str(ERR_104), status=400, mimetype='application/json')
        return resp
    result = query_db("select BGPPrefix,PrefixLocation from BGPPrefixGeo where BGPPrefix = \'"+bgpprefix+"/"+leng+"\' and GeoDate like \'"+geodate+"%%\'")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp

@app.route("/<geodate>/prefixes_announced_from_asn/", methods=['GET'])
def Oprefixes_announced_from_asn(geodate):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_105), status=400, mimetype='application/json')
    return resp
@app.route('/<geodate>/prefixes_announced_from_asn/<asn>', methods=['GET'])
def prefixes_announced_from_asn(geodate,asn):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    if not checkint(asn):
        resp = Response(str(ERR_103), status=400, mimetype='application/json')
        return resp
    result = query_db("select OriginAS,BGPPrefix,PrefixLocation from BGPPrefixGeo where OriginAS= "+asn+" and GeoDate like \'"+geodate+"%%\'")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp

@app.route("/<geodate>/prefixes_announced_from_country/", methods=['GET'])
def Oprefixes_announced_from_country(geodate):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_106), status=400, mimetype='application/json')
    return resp
@app.route('/<geodate>/prefixes_announced_from_country/<country>', methods=['GET'])
def prefixes_announced_from_country(geodate,country):
    countryStr="{\\'"+country+"\\'}"
    result = query_db("select OriginAS,BGPPrefix,PrefixLocation from BGPPrefixGeo where PrefixLocation like \'"+countryStr+"\' and GeoDate like \'"+geodate+"%%\'")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp

@app.route("/<geodate>/bgp_prefixes_num_countries/", methods=['GET'])
def Obgp_prefixes_num_countries(geodate):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_107), status=400, mimetype='application/json')
    return resp
@app.route('/<geodate>/bgp_prefixes_num_countries/<num>', methods=['GET'])
def bgp_prefixes_num_countries(geodate,num):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    if not checkint(num[1:]):
        resp = Response(str(ERR_107), status=400, mimetype='application/json')
        return resp
    result = query_db("select BGPPrefix,PrefixLocation from BGPPrefixGeo where PrefixLocation != 'set()' \
    and round(char_length(PrefixLocation) - char_length(REPLACE ( PrefixLocation, \'\,\', '')) + 1 ) "+num+" and GeoDate like \'"+geodate+"%%\'")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp


@app.route("/<geodate>/slash24_prefixes_num_countries/", methods=['GET'])
def Oslash24_prefixes_num_countries(geodate):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_108), status=400, mimetype='application/json')
    return resp
@app.route('/<geodate>/slash24_prefixes_num_countries/<num>', methods=['GET'])
def slash24_prefixes_num_countries(geodate,num):
    if not check(geodate):
        resp = Response(str(ERR_101), status=400, mimetype='application/json')
        return resp
    if not checkint(num[1:]):
        resp = Response(str(ERR_108), status=400, mimetype='application/json')
        return resp
    result = query_db("select Sub24Block IPBlock,BlockLocation from BlockGeo where BlockLocation != 'set()' and round(char_length(BlockLocation) - char_length(REPLACE ( BlockLocation, \'\,\', '')) + 1 ) "+num+" and GeoDate like \'"+geodate+"%%\'")
    data = json.dumps(result)
    resp = Response(data, status=200, mimetype='application/json')
    return resp

@app.route("/<geodate>/<unknown>", methods=['GET'])
def geodate_and_unknown(geodate,unknown):
    if not check(geodate):
        err=str([ERR_101,ERR_102])
        resp = Response(err, status=400, mimetype='application/json')
        return resp
    resp = Response(str(ERR_102), status=400, mimetype='application/json')
    return resp

@app.errorhandler(404)
def page_not_found(e):
    resp = Response(str(ERR_102), status=400, mimetype='application/json')
    return resp

#if __name__ == "__main__":
#    app.run(host='***',port=8080,threaded=True,debug= True)
#    app.run()
