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
    oldstate=''
    for state,mtime in zip(states,times):
        if state==oldstate and state=='defined': #this is to remove the first "defined" when the new one comes
            res.pop()
        oldstate=state
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

@outputSchema('SKIP:int')
def Skipped(bag):
    if bag is None:
        return(1)
    else:
        return(0)
        
@outputSchema('TIMESTAMP:long')
def Tstamp(bag):
    if bag is None:
        return(0)
    else:
        for JOBSTATUS, t  in bag:
            return(t)

@outputSchema('SORT:int')
def Sorted(bag):
    if bag is None:
        return(1)
    minTime=0
    SORTED=0
    for JOBSTATUS, t  in bag:
        if t<minTime:SORTED=1
        minTime=t
    return SORTED

@outputSchema('intervals:bag{i:(status:chararray, time:long)}')
def AllTheTimes(bag):

    if bag is None:
        return ([])  #skip = 1 

    st=[]
    ti=[]
    inState={}
    res=[]
    for JOBSTATUS, t  in bag:
        st.append(JOBSTATUS)
        ti.append(t)

    for i in range(len(st)-1):
        if st[i] in inState:
            inState[st[i]]+=ti[i+1]-ti[i]
        else:
            inState[st[i]]=ti[i+1]-ti[i]
    for i in inState:
        res.append((i,inState[i]/1000))
    return res

@outputSchema('stimes:tuple( inPending:long, inDefined:long, inActivated:long, inSent:long,inStarting:long, inRunning:long, inHolding:long, inMerging:long)')
def TheTimes(bag):

    if bag is None:
        return ()  #skip = 1 
    
    inPending=inDefined=inActivated=inSent=inStarting=inRunning=inHolding=inMerging=None
    
    st=[]
    ti=[]
    inState={}
    res=[]
    for JOBSTATUS, t  in bag:
        st.append(JOBSTATUS)
        ti.append(t)

    for i in range(len(st)-1):
        if st[i] in inState:
            inState[st[i]]+=ti[i+1]-ti[i]
        else:
            inState[st[i]]=ti[i+1]-ti[i]
            
    if 'pending' in inState:  inPending=inState['pending']/1000
    if 'defined'in inState:   inDefined=inState['defined']/1000
    if 'activated'in inState: inActivated=inState['activated']/1000
    if 'sent'in inState:      inSent=inState['sent']/1000
    if 'starting'in inState:  inStarting=inState['starting']/1000
    if 'running'in inState:   inRunning=inState['running']/1000
    if 'holding'in inState:   inHolding=inState['holding']/1000
    if 'merging'in inState:   inMerging=inState['merging']/1000
         
                
    return (inPending, inDefined, inActivated, inSent, inStarting, inRunning, inHolding, inMerging)

    # if 'pending' in d and 'defined' in d and d['pending']<=d['defined']:
    #     inPending=(d['defined']-d['pending'])/1000
    # else:
    #     SKIP|=1<<0
    #
    # if 'activated' in d and 'defined' in d and d['activated']>=d['defined']:
    #     inDefined=(d['activated']-d['defined'])/1000
    # else:
    #     SKIP|=1<<1
    #
    # if 'activated' in d and 'sent' in d and d['sent']>=d['activated']:
    #     inActivated=(d['sent']-d['activated'])/1000
    # else:
    #     SKIP|=1<<2
    #
    # if 'starting' in d and 'sent' in d and d['starting']>=d['sent']:
    #     inSent=(d['starting']-d['sent'])/1000
    # else:
    #     SKIP|=1<<3
    #
    #
    # if 'running' in d and 'starting' in d and d['running']>=d['starting']:
    #     inStarting=(d['running']-d['starting'])/1000
    # else:
    #     SKIP|=1<<4
    #
    # if 'holding' in d and 'running' in d and d['holding']>=d['running']:
    #     inRunning=(d['holding']-d['running'])/1000
    # else:
    #     SKIP|=1<<5
    #
    # if 'holding' in d and 'finished' in d and d['finished']>=d['holding']:
    #     inHolding=(d['finished']-d['holding'])/1000
    # else:
    #     SKIP|=1<<6
    #
    # if 'merging' in d and 'finished' in d and d['finished']>=d['merging']:
    #     inMerging=(d['finished']-d['merging'])/1000
    # else:
    #     SKIP|=1<<7
    #
    # if 'cancelled' in d:
    #     if 'holding' in d and d['cancelled']>=d['holding']:
    #         inHolding=(d['cancelled']-d['holding'])/1000
    #     elif 'running' in d and d['cancelled']>=d['running']:
    #         inRunning=(d['cancelled']-d['running'])/1000
    #     elif 'starting' in d and d['cancelled']>=d['starting']:
    #         inStarting=(d['cancelled']-d['starting'])/1000
    #     elif 'sent' in d and d['cancelled']>=d['sent']:
    #         inSent=(d['cancelled']-d['sent'])/1000
    #     elif 'activated' in d and d['cancelled']>=d['activated']:
    #         inActivated=(d['cancelled']-d['activated'])/1000
    #     elif 'defined' in d and d['cancelled']>=d['defined']:
    #         inDefined=(d['cancelled']-d['defined'])/1000
    #     elif 'pending' in d and d['cancelled']>=d['pending']:
    #         inPending=(d['cancelled']-d['pending'])/1000
    #
    # if 'failed' in d:
    #     if 'holding' in d and d['failed']>=d['holding']:
    #         inHolding=(d['failed']-d['holding'])/1000
    #     elif 'running' in d and d['failed']>=d['running']:
    #         inRunning=(d['failed']-d['running'])/1000
    #     elif 'starting' in d and d['failed']>=d['starting']:
    #         inStarting=(d['failed']-d['starting'])/1000
    #     elif 'sent' in d and d['failed']>=d['sent']:
    #         inSent=(d['failed']-d['sent'])/1000
    #     elif 'activated' in d and d['failed']>=d['activated']:
    #         inActivated=(d['failed']-d['activated'])/1000
    #     elif 'defined' in d and d['failed']>=d['defined']:
    #         inDefined=(d['failed']-d['defined'])/1000
    #     elif 'pending' in d and d['failed']>=d['pending']:
    #         inPending=(d['failed']-d['pending'])/1000
    #
    # return (SORTED,SKIP,inPending,inDefined,inActivated,inSent,inStarting,inRunning,inHolding,inMerging)
    #
    
    


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