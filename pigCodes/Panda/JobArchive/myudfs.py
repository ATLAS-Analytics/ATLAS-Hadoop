@outputSchema('tuple( timeGetJob:integer, timeStageIn:integer, timeExe:integer, timeStageOut:integer, timeSetup:integer)')
def deriveTimes(origString):
    times=origString.split('|')
    return times
    
@outputSchema('tuple( walltime:integer, cpueff:float, queue_time:integer)')
def deriveDurationAndCPUeff(origTuple):
    walltime=origTuple[2]-origTuple[1]
    if walltime>0: cpueff = origTuple[3]/walltime
    queue_time=origTuple[1]-origTuple[0]
    return (walltime,cpueff,queue_time)
    
