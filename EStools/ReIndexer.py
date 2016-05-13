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

sources=[]
for d in range(1:32):
    sources.append( 'faxcost-2016.1.'+str(d) )
    
destination=['faxcost-2016.01']

for s in sources:
    print(s)
    res = helpers.reindex(es, s, destination)

print 'All done.'