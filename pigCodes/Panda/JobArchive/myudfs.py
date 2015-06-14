from datetime import datetime

@outputSchema('tuple( timeGetJob:int, timeStageIn:int, timeExe:int, timeStageOut:int, timeSetup:int)')
def deriveTimes(origString):
    if origString is None:
        return (0,0,0,0,0)
    times=origString.split('|')
    return (int(times[0]),int(times[1]),int(times[2]),int(times[3]),int(times[4]))
    
@outputSchema('tuple( walltime:int, cpueff:float, queue_time:int)')
def deriveDurationAndCPUeff(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME):
    if CREATIONTIME is None or STARTTIME is None or ENDTIME is None or CPUCONSUMPTIONTIME is None:
        return (0,0.0,0)
    CREATIONTIME = CREATIONTIME / 1000
    STARTTIME = STARTTIME / 1000
    ENDTIME = ENDTIME / 1000
    
    walltime = ENDTIME-STARTTIME
    queue_time  =STARTTIME-CREATIONTIME
    
    cpueff=0
    try:
        if walltime>0 and CPUCONSUMPTIONTIME!='':
            cpueff = float(CPUCONSUMPTIONTIME)/walltime
    except:
        print "problem with cpueff: "+CPUCONSUMPTIONTIME
        
    return (walltime,cpueff,queue_time)
    
@outputSchema('TIMESTAMP:chararray')
def Tstamp(ts):
    if ts is None:
        return(0)
    else:
        return(datetime.fromtimestamp(ts/1000).isoformat())