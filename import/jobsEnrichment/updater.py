#!/usr/bin/env python3

# this code updates child_id info on all jobs that have been retried

import os, sys
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan, bulk

import numpy as np
import pandas as pd

infile='2017-08-12'

df = pd.read_csv('/tmp/' + infile, header=None, names=['old_pid','new_pid','relation_type'])
print(df.head())

# leave only retries
df=df[df.relation_type == 'retry']
del df['relation_type']

#find min and max old pid
min_pid = df.old_pid.min()
max_pid = df.old_pid.max()

#find oldest and newest index that should be scanned
es = Elasticsearch([{'host':'atlas-kibana.mwt2.org', 'port':9200}],timeout=60)

min_limit_q = {
    "size": 1,
    'query':{
        "range": { 
            "pandaid": {
                "lte":int(min_pid), 
                "gt":0  
            }
        }
    },
    "sort": [ { "pandaid": { "order": "desc" } } ] 
}

r_min = es.search(index="jobs_archive_2017*",body=min_limit_q)
min_index = r_min['hits']['hits'][0]['_index']

max_limit_q = {
    "size": 1,
    'query':{
        "range": { 
            "pandaid": {
                "lte":int(max_pid + 10E6), 
                "gt":int(max_pid)  
            }
        }
    },
    "sort": [ { "pandaid": { "order": "asc" } } ] 
}

r_min = es.search(index="jobs_archive_2017*",body=max_limit_q)
max_index = r_min['hits']['hits'][0]['_index']

print("limit indices: ",min_index, max_index)

# get relevant job indices
indices = es.cat.indices(index="jobs_archive_20*", h="index", request_timeout=600).split('\n')
indices = sorted(indices)
indices = [x for x in indices if x != '']

selected_indices=[]
acc = False
for i in indices:
    if i == min_index: 
        acc = True
        selected_indices.append(i)
        continue
    if i == max_index: acc = False
    if acc == True:
        selected_indices.append(i)

job_indices = ''
job_indices = ','.join(selected_indices)
print(job_indices)

job_query = {
    "size": 0,
    "_source": ["_id"],
    'query':{
        "match_all": {}
        # maybe loop over over not finished jobs
        #    'bool':{ 'must':[  { "term": {"jobstatus": "finished" } }  ]  }
    }
                
}

def simple_update(jobs):
    global df
    df = df.reset_index(drop=True)
    toClean = []
    data = []     
    for j in jobs:
        pid=j['pid']
        ind=j['ind']
        found = df[df.old_pid == pid]
        if found.shape[0]==0: continue
        if found.shape[0]>1:
            print(found)
        d = {
            '_op_type': 'update',
            '_index': ind,
            '_type': 'jobs_data',
            '_id': pid,
            'doc': {'child_ids': found.new_pid.tolist()}
        }    
        data.append(d)
        toClean+=found.index.tolist()
    print('df:',df.shape[0], 'found:', len(data))
    df=df.drop(df.index[toClean])
    res = bulk(client=es, actions=data, stats_only=True, timeout="5m", size=10000)
    print(res)

jobs=[]
scroll = scan(client=es, index=job_indices, query=job_query, scroll='5m', timeout="5m", size=10000)
count = 0

for res in scroll:
    count += 1    
    if not count%100000: 
        print(count, ' selected:', count)    
        simple_update(jobs)
        jobs=[]
    #print(res)
    jobs.append({"pid":int(res['_id']), "ind":res['_index']})
    #if count%5 == 1: exec_update(jobs)
    if count>10000000: break

simple_update(jobs)
jobs=[]







# def exec_update(jobs):
#     global df
#     df = df.reset_index(drop=True)
#     print ('df:', df.shape)
#     jdf=pd.DataFrame(jobs)
#     jdf.set_index('pid',inplace=True)
#     print('jdf:',jdf.shape)
#     #print(jdf.head())
#     j=df.join(jdf, on='old_pid', how='inner') 
#     print(j)
#     print('j:', j.shape)
#     # drop ones we matched
#     td = j.index.tolist()
#     df = df.drop(df.index[td])