# serial performance client at UC - CL ES  82.26s user 12.98s system 3% cpu 40:47.27 total
# 50 streams sleep 100ms                  117.18s user 27.90s system 51% cpu 4:40.61 total
from datetime import datetime
from elasticsearch import Elasticsearch

import threading
from threading import Thread
import  subprocess, Queue, os, sys,time

nThreads=1


def worker():
    while True:
        st=q.get()
        res = es.search(index="network_weather-2015-10-11", body=st, size=1000)
        print "records:",res['hits']['total'], "\t remaining:",q.qsize()
        # time.sleep(0.1)
        q.task_done()
        
print "make sure we are connected right."
import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])

print "documents to look into:"
print es.count(index="network_weather-2015-10-11")

usrc={
    "size": 0, 
    "aggregations": {
       "unique_vals": {
          "terms": {
             "field": "@message.src",
             "size":1000
          }
       }
    }
}
udest={
    "size": 0, 
    "aggregations": {
       "unique_vals": {
          "terms": {
             "field": "@message.dest",
             "size":1000
          }
       }
    }
}
usrcs=[]
udests=[]

res = es.search(index="network_weather-2015-10-11", body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    usrcs.append(tag['key'])

res = es.search(index="network_weather-2015-10-11", body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    udests.append(tag['key'])

print "unique sources: ", len(usrcs)
print "unique destinations: ", len(udests)

q=Queue.Queue()
for i in range(nThreads):
    t = Thread(target=worker)
    t.daemon = True
    t.start()

for s in usrcs:
    for d in udests:
        st={
        "query": {
                "filtered":{
                    "query": {
                        "match_all": {}
                    },
                    "filter":{
                        "and": [
                            {
                                "term":{ "@message.src":s }
                            },
                            {
                                "term":{ "@message.dest":d }
                            }
                        ]
                    }
                }
            }
        }
        q.put(st)


q.join()

print 'All done.'

# res = es.search(index="network_weather-2015-10-11", body=st, size=10000)
# print("Got %d hits." % res['hits']['total'])
#
# for hit in res['hits']['hits']:
#     print hit['_source']['@timestamp'],"\ttype:", hit['_type'], "\tsource:",hit['_source']['@message']['src'], "\tdestination:",hit['_source']['@message']['dest'],
#     if hit['_type']=='packet_loss_rate':
#         print "\t packet_loss:",hit['_source']['@message']['packet_loss']
#     elif hit['_type']=='latency':
#         print "\t delay_mean:",hit['_source']['@message']['delay_mean'], "\tdelay_sd:",hit['_source']['@message']['delay_sd']
#     elif hit['_type']=='throughput':
#         print "\t throughput:",hit['_source']['@message']['throughput']
    