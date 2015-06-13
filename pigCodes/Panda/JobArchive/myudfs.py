@outputSchema('tuple( timeGetJob:int, timeStageIn:int, timeExe:int, timeStageOut:int, timeSetup:int)')
def deriveTimes(origString):
    times=origString.split('|')
    return times
    
@outputSchema('tuple( walltime:int, cpueff:float, queue_time:int)')
def deriveDurationAndCPUeff(origTuple):
    walltime=origTuple[2]-origTuple[1]
    if walltime>0: cpueff = origTuple[3]/walltime
    queue_time=origTuple[1]-origTuple[0]
    return (walltime,cpueff,queue_time)
    
