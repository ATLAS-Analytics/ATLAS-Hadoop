from datetime import datetime
import time

def strToSDOT(d):
    return d.replace(' ','T')+'Z'

@outputSchema( 'tuple(protocol:chararray, dst_rse:chararray, started_at:chararray, scope:chararray, transferred_at:chararray, src_type:chararray, dst_type:chararray, submitted_at:chararray, name:chararray, request_id:chararray, bytes:long, activity:chararray, src_rse:chararray )')
def splitPayload(pl):
    byt=0
    try:
         byt=int(pl['bytes'])
    except:
         pass
    started = strToSDOT(pl['started_at'])
    transfe = strToSDOT(pl['transferred_at'])
    submite = strToSDOT(pl['submitted_at'])
    return (pl['protocol'],pl['dst-rse'],started,pl['scope'],transfe,pl['src-type'],pl['dst-type'],submite,pl['name'],pl['request-id'],byt,pl['activity'],pl['src-rse'])


@outputSchema( 'started_at:int')
def checkDate(pl):
    st=pl['started_at']
    date=st.split(' ')
    [y,m,d]=date[0].split('-')
    if int(m)!=2: return 0
    if int(d)>7: return 0
    return 1
