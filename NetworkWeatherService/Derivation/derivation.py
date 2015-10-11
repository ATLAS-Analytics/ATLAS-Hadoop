from datetime import datetime
from elasticsearch import Elasticsearch
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
                        "term":{
                            "@message.src":"192.41.230.59"
                        }
                    },
                    {
                        "term":{
                            "@message.dest":"134.158.20.192"
                        }
                    }
                ]
            }
        }
    }
}
st={
"query": {
        "filtered":{
            "query": {
                "term":{ "@message.src":"192.41.230.59"}
            }
        }
    }
}
res = es.search(index="network-weather-2015-10-11", body=st)
print("Got %d Hits:" % res['hits']['total'])
#for hit in res['hits']['hits']:
#    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])