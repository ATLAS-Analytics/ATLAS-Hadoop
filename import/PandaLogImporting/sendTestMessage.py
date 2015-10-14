#!/usr/bin/python

import requests
import json
import time

url='http://aianalytics05.cern.ch:8081'

arr=[{
  "headers" : {
             "timestamp" : int(time.time())*1000,
             "host" : "url"
             },
  "body": "{\"relativeCreated\": 36.53407096862793, \"process\": 5681, \"module\": \"logtest\"}"
 }
]

for i in range(100):
    r=requests.post(url, data=json.dumps(arr))

