@outputSchema('tuple( timeGetJob:int, timeStageIn:int, timeExe:int, timeStageOut:int, timeSetup:int)')
def deriveTimes(origString):
    times=origString.split('|')
    return times
    
@outputSchema('tuple( walltime:int, cpueff:float, queue_time:int)')
def deriveDurationAndCPUeff(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME):
    walltime=ENDTIME-STARTTIME
    cpueff=0
    # try:
    #     if walltime>0 and CPUCONSUMPTIONTIME!='':
    #         cpueff = int(CPUCONSUMPTIONTIME)/walltime
    # except:
    #     print "problem with cpueff: "+CPUCONSUMPTIONTIME
    queue_time=STARTTIME-CREATIONTIME
    return (walltime,cpueff,queue_time)
    
