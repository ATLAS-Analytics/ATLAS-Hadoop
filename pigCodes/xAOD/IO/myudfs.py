#!/usr/bin/python
from datetime import datetime
import json

@outputSchema('trans:bag{t:(status:chararray, time:long)}')
def BagToBag(bag):
    states = []
    times = []
    res = []
    for PANDAID, MODIFICATIONTIME, JOBSTATUS, PRODSOURCELABEL, CLOUD, COMPUTINGSITE  in bag:
        toIns=-1
        for i in range(len(states)):
            if MODIFICATIONTIME<times[i]:
                toIns=i
                break
        if toIns!=-1:
            states.insert(toIns,JOBSTATUS)
            times.insert(toIns,MODIFICATIONTIME)
        else:
            states.append(JOBSTATUS)
            times.append(MODIFICATIONTIME)
    oldstate=''
    for state,mtime in zip(states,times):
        if state==oldstate and state=='defined': #this is to remove the first "defined" when the new one comes
            res.pop()
        oldstate=state
        res.append((state,mtime))
    return res


@outputSchema("ip:chararray,timeentry:long,ROOT_RELEASE:chararray,ReadCalls:int,ReadSize:int,CacheSize:int")
def decode(line):
    d = json.loads(line)
    IP=d['ip']
    TIMEENTRY=d['timeentry']
    ROOT_RELEASE=d['ROOT_RELEASE']
    READCALLS=d['ReadCalls']
    READSIZE=d['ReadSize']
    CACHESIZE=d['CacheSize']
    return (IP,TIMEENTRY,ROOT_RELEASE,READCALLS,READSIZE,CACHESIZE)
