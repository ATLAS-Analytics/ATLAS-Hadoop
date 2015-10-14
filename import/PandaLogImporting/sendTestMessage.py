#!/usr/bin/python

import requests
import json
import time

url='http://aianalytics05.cern.ch:8081'

arr=[{
  "headers" : {
             "timestamp" : int(time.time())*1000,
             "host" : url
             },
  "body": "{'relativeCreated': 36.53407096862793, 'process': 5681, 'module': 'logtest', 'funcName': '<module>', 'message': 'This is only a test', 'Type': 'retryModule', 'Timestamp': '2015-08-11 15:36:05.082240', 'filename': 'logtest.py', 'levelno': 10, 'processName': 'MainProcess', 'lineno': 6, 'asctime': '2015-08-11 15:36:05,082', 'msg': 'This is only a test', 'User': '', 'args': [], 'exc_text': null, 'ID': '', 'PandaID': 0, 'name': 'panda.mon.dev', 'thread': 140735210226432, 'created': 1439307365.082483, 'threadName': 'MainThread', 'msecs': 82.48305320739746, 'pathname': 'logtest.py', 'exc_info': null, 'levelname': 'DEBUG'}"
 }
]

r=requests.post(url, data=json.dumps(arr))

