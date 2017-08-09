import urllib2
import pickle
import sys
try: import simplejson as json
except ImportError: import json

site_map={}

try:
    url='http://atlas-agis-api.cern.ch/request/site/query/list/?json&vo_name=atlas&state=ACTIVE'
    res=json.load(urllib2.urlopen(url))
    for s in res:
        site_name=s["rc_site"]
        for rse in s['ddmendpoints']:
            site_map[rse] = site_name
    print('Sites reloaded.')
except:
    print ("Could not get sites from AGIS. Exiting...")
    print ("Unexpected error: ", str(sys.exc_info()[0]))

pickle.dump(site_map,  open( "mapping.p", "wb" ))
print("AGIS data saved.")