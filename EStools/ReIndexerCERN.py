#!/usr/bin/env python

import os, sys, time

from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection,  exceptions as es_exceptions, helpers

es = Elasticsearch([{'host':'atlas-kibana.mwt2.org', 'port':9200},{'host':'cl-analytics.mwt2.org', 'port':9200}], timeout=600)
ce = Elasticsearch([{'host':'es-atlas.cern.ch', 'port':9202}],http_auth=('es-atlas', 'yourpassword'), timeout=600)
sources=[]
sources.append( 'faxcost-2016.01.01-10')

for s in sources:
    print(s)
    res = helpers.reindex(es, s, s,target_client=ce, chunk_size=5000)

print 'All done.'