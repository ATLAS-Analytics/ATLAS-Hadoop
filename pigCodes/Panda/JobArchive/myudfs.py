@outputSchema('tuple( timeGetJob:int, timeStageIn:int, timeExe:int, timeStageOut:int, timeSetup:int)')
def deriveTimes(origString):
    times=origString.split('|')
    return times
    
@outputSchema('tuple( walltime:int, cpueff:float, queue_time:int)')
def deriveDurationAndCPUeff(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME):
    walltime=ENDTIME-STARTTIME
    if walltime>0: 
        cpueff = CPUCONSUMPTIONTIME/walltime
    else:
        cpueff = 0
    queue_time=STARTTIME-CREATIONTIME
    return (walltime,cpueff,queue_time)
    
