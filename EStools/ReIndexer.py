#!/usr/bin/env python

import os, sys, time

from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection,  exceptions as es_exceptions, helpers

es = Elasticsearch([{'host':'atlas-kibana.mwt2.org', 'port':9200},{'host':'cl-analytics.mwt2.org', 'port':9200}], sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60, timeout=600)

sources=[]
for d in range(1,11):
    sources.append( 'faxcost-2016.1.'+str(d) )

destination='faxcost-2016.01.01-10'

for s in sources:
    print(s)
    res = helpers.reindex(es, s, destination)

print 'All done.'