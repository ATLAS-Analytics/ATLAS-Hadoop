@outputSchema('tuple( timeGetJob:int, timeStageIn:int, timeExe:int, timeStageOut:int, timeSetup:int)')
def deriveTimes(origString):
    if origString is None:
        return (0,0,0,0,0)
    times=origString.split('|')
    return (int(times[0]),int(times[1]),int(times[2]),int(times[3]),int(times[4]))
    
@outputSchema('tuple( walltime:int, cpueff:float, queue_time:int)')
def deriveDurationAndCPUeff(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME):
    walltime=cpueff=queue_time=0
    if ENDTIME is None or  CREATIONTIME is None or STARTTIME is None or ENDTIME is None or CPUCONSUMPTIONTIME is None:
        return (walltime,cpueff,queue_time)
    walltime = ENDTIME-STARTTIME
    try:
        if walltime>0 and CPUCONSUMPTIONTIME!='':
            cpueff = int(CPUCONSUMPTIONTIME)/walltime
    except:
        print "problem with cpueff: "+CPUCONSUMPTIONTIME
    queue_time  =STARTTIME-CREATIONTIME
    return (walltime,cpueff,queue_time)
    
