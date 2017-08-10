from datetime import datetime
import time
import pickle

site_map={}

def siteFromRSE(RSE):
    global site_map
    if len(site_map)==0:
        site_map = pickle.load( open( "/afs/cern.ch/user/i/ivukotic/public/mapping.p", "rb" ) )
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
