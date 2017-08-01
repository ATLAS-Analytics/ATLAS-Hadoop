from datetime import datetime
import time
import requests
try: import simplejson as json
except ImportError: import json

site_map={}

def loadMapping():
    try:
        r=requests.get('http://atlas-agis-api.cern.ch/request/site/query/list/?json&vo_name=atlas&state=ACTIVE')
        res = r.json()
        for s in res:
            site_name=s["rc_site"]
            for rse in s['ddmendpoints']:
                site_map[rse] = site_name
        print('Sites reloaded.')
    except:
        print ("Could not get sites from AGIS. Exiting...")
        print ("Unexpected error: ", str(sys.exc_info()[0]))

def siteFromRSE(RSE):
    if len(site_map)==0: loadMapping()
    if RSE in site_map:
        return (site_map[RSE])
    return "unknown"

def strToSDOT(d):
    return d.replace(' ','T')+'Z'

@outputSchema( 'tuple(protocol:chararray, dst_rse:chararray, started_at:chararray, scope:chararray, transferred_at:chararray, src_type:chararray, dst_type:chararray, submitted_at:chararray, name:chararray, request_id:chararray, bytes:long, activity:chararray, src_rse:chararray, dst:chararray, src:chararray )')
def splitPayload(pl):
    byt=0
    try:
         byt=int(pl['bytes'])
    except:
         pass
    started = strToSDOT(pl['started_at'])
    transfe = strToSDOT(pl['transferred_at'])
    submite = strToSDOT(pl['submitted_at'])
    dst=siteFromRSE(pl['dst-rse'])
    src=siteFromRSE(pl['src-rse'])
    return (pl['protocol'],pl['dst-rse'],started,pl['scope'],transfe,pl['src-type'],pl['dst-type'],submite,pl['name'],pl['request-id'],byt,pl['activity'],pl['src-rse'],dst,src)


@outputSchema( 'started_at:int')
def checkDate(pl):
    st=pl['started_at']
    date=st.split(' ')
    [y,m,d]=date[0].split('-')
    if int(m)!=2: return 0
    if int(d)>7: return 0
    return 1
