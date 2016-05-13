#!/usr/bin/env python

import os, sys, time
import requests

from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

lastReconnectionTime=0

def GetESConnection(lastReconnectionTime):
    if ( time.time()-lastReconnectionTime < 60 ): 
        return
    lastReconnectionTime=time.time()
    print "make sure we are connected right..."
    res = requests.get('http://cl-analytics.mwt2.org:9200')
    print(res.content)
    
    es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])
    return es



es = GetESConnection(lastReconnectionTime)
while (not es):
    es = GetESConnection(lastReconnectionTime)

sources=['faxcost-2016.4.27','faxcost-2016.4.28']
destination=['faxcost-2016.04']

for source in sources:
    print 'doing:', source
    try:
        res = helpers.bulk(es, source, destination, raise_on_exception=True)
        print (i.name, "\t inserted:",res[0], '\tErrors:',res[1])
        aLotOfData=[]
    except es_exceptions.ConnectionError as e:
        print 'ConnectionError ', e
    except es_exceptions.TransportError as e:
        print 'TransportError ', e
    except helpers.BulkIndexError as e:
        print e[0]
        print len(e[1])
    except:
        print ('Something seriously wrong happened. ', sys.exc_info()[0])

print 'All done.'