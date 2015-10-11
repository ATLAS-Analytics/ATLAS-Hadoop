from datetime import datetime
from elasticsearch import Elasticsearch
import timeit

import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)


es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])
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


print es.count(index="network_weather-2015-10-11")

timeit.timeit('res = es.search(index="network_weather-2015-10-11", body=st, size=10000)')
print("Got %d hits." % res['hits']['total'])
for hit in res['hits']['hits']:
    print hit['_source']['@timestamp'],"\ttype:", hit['_type'], "\tsource:",hit['_source']['@message']['src'], "\tdestination:",hit['_source']['@message']['dest'],
    if hit['_type']=='packet_loss_rate': 
        print "\t packet_loss:",hit['_source']['@message']['packet_loss']
    elif hit['_type']=='latency': 
        print "\t delay_mean:",hit['_source']['@message']['delay_mean'], "\tdelay_sd:",hit['_source']['@message']['delay_sd']
    elif hit['_type']=='throughput': 
        print "\t throughput:",hit['_source']['@message']['throughput']
    