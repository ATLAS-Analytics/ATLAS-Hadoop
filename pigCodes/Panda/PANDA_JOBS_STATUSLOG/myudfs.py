#!/usr/bin/python
from datetime import datetime

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
    for state,mtime in zip(states,times):
        res.append((state,mtime))
    return res

@outputSchema('trans:bag{t:(status:chararray)}')
def OnlyStates(bag):
    states = []
    times = []
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
    # removes repeating states
    res=[]
    prev=''
    for i in states:
        if i==prev: 
            continue
        prev=i
        res.append(i)
    return res




# holding    -> transferring
# trasfering -> finished

@outputSchema("calc:tuple(SKIP:int,RESULT:long,LASTMODIFIED:long)")
def HoldingToTransferringTimes(bag):
    SKIP=0
    RESULT=0L
    HOLDING=0
    TRANSFERRING=0
    LASTMODIFIED=-1L
    d = {}
    for JOBSTATUS, t  in bag:
        LASTMODIFIED=t  #since these are ordered
        if JOBSTATUS!='holding' and JOBSTATUS!='transferring': continue
        if JOBSTATUS=='holding': HOLDING+=1
        if JOBSTATUS=='transferring':  
            TRANSFERRING+=1
            if TRANSFERRING>1: continue
        d[JOBSTATUS] = t
        
    if TRANSFERRING==0 and HOLDING!=1:  
        SKIP=1
        return (SKIP,RESULT,LASTMODIFIED)
    if TRANSFERRING==0:
        SKIP=2
        return (SKIP,RESULT,LASTMODIFIED)
    if HOLDING!=1: 
        SKIP=3
        return (SKIP,RESULT,LASTMODIFIED)

    RESULT=long(d['transferring']-d['holding'])/1000
    
    if RESULT<0: 
        SKIP=5 
        return (SKIP,0L,LASTMODIFIED)
        
    return (SKIP,RESULT,LASTMODIFIED)