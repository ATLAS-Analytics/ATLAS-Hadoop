#!/usr/bin/python
from datetime import datetime


# pending defined activated sent starting running holding finished
# gets bag with state,time but returns intervals

@outputSchema("calc:tuple(SKIP:int,inPending:long,inDefined:long,inActivated:long,inSent:long,inStarting:long,inRunning:long,inHolding:long)")
def AllTheTimes(bag):
         
    #these will be intervals
    inPending=0L
    inDefined=0L
    inActivated=0L
    inSent=0L
    inStarting=0L
    inRunning=0L
    inHolding=0L
    SKIP=0
    
    
    if bag is None:
        return (SKIP,inPending,inDefined,inActivated,inSent,inStarting,inRunning,inHolding)
        
    d = {}
    for JOBSTATUS, t  in bag:
        d[JOBSTATUS] = t
    
    if 'pending' in d and 'defined' in d and d['pending']<=d['defined']:
        inPending=(d['defined']-d['pending'])/1000
    else: 
        SKIP|=1<<0
 
    if 'activated' in d and 'defined' in d and d['activated']>=d['defined']:
        inDefined=(d['activated']-d['defined'])/1000
    else: 
        SKIP|=1<<1
 
    if 'activated' in d and 'sent' in d and d['sent']>=d['activated']:
        inActivated=(d['sent']-d['activated'])/1000
    else: 
        SKIP|=1<<2
        
 
    if 'starting' in d and 'sent' in d and d['starting']>=d['sent']:
        inSent=(d['starting']-d['sent'])/1000
    else: 
        SKIP|=1<<3
        
 
    if 'running' in d and 'starting' in d and d['running']>=d['starting']:
        inStarting=(d['running']-d['starting'])/1000
    else: 
        SKIP|=1<<4
 
    if 'holding' in d and 'running' in d and d['holding']>=d['running']:
        inRunning=(d['holding']-d['running'])/1000
    else: 
        SKIP|=1<<5
    
    if 'holding' in d and 'finished' in d and d['finished']>=d['holding']:
        inHolding=(d['finished']-d['holding'])/1000
    else: 
        SKIP|=1<<6
    
    return (SKIP,inPending,inDefined,inActivated,inSent,inStarting,inRunning,inHolding)

