from datetime import datetime
from elasticsearch import Elasticsearch

import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])

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
usrcs=udests=[]

res = es.search(index="network_weather-2015-10-11", body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    usrcs.append(tag['key'])

res = es.search(index="network_weather-2015-10-11", body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    udests.append(tag['key'])

print "unique sources: ", len(usrcs)
print "unique destinations: ", len(udests)

st={
"query": {
        "filtered":{
            "query": {
                "match_all": {}
            },
            "filter":{
                "and": [
                    {
                        "term":{ "@message.src":"192.41.230.59" }
                    },
                    {
                        "term":{ "@message.dest":"134.158.20.192" }
                    }
                ]
            }
        }
    }
}


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
        res = es.search(index="network_weather-2015-10-11", body=st, size=10000)
        print "source:", s,"\tdestination:",d,"\trecords:",res['hits']['total']  


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
    