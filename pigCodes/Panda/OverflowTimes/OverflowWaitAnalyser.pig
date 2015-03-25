-- this code select jobs in interval given by the input directory that run in certain ANALY queues and calculates:
-- total job counts
-- average duration of job (from the moment of submission untill the end)
-- average time till running (from the moment of submission)
-- all of it grouped by TransferType, ComputingSite and JobsStatus

REGISTER '/usr/lib/pig/piggybank.jar' ;

-- regular jars
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';


REGISTER 'myudfs.py' using jython as myfuncs;

PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.2015-03-1*' USING AvroStorage();
PA = filter PAN by COMPUTINGSITE=='ANALY_MWT2_SL6' and JOBSTATUS=='finished';
P = foreach PA generate (long)PANDAID, TRANSFERTYPE,(long)MODIFICATIONTIME, JOBSTATUS,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, (int)ATTEMPTNR, REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, (int)PILOTERRORCODE, (long)INPUTFILEBYTES, (long)CPUCONSUMPTIONTIME,  ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME;

FLOW = LOAD 'MWT2overflows' as (PANDAID:long, COMPUTINGSITE:chararray, PRODSOURCELABEL:chararray, times:bag{tuple(state:chararray, time:long)});

X = JOIN P by PANDAID, FLOW by PANDAID;
--L = LIMIT X 100; dump L;

-- here I have to go through times bag of X, and using a UDF calculate time differences. 
REC = foreach X generate P::PANDAID, P::TRANSFERTYPE, P::JOBSTATUS, P::ATTEMPTNR, WAITTIME/1000 as OWAIT, FLATTEN(myfuncs.AllTheTimes(times)); 
L = LIMIT REC 1000; dump L;

grAv = GROUP REC BY(P::TRANSFERTYPE, calc::SKIP);
avints = FOREACH grAv GENERATE group, COUNT(REC.P::JOBSTATUS), AVG(REC.OWAIT), AVG(REC.calc::inPending), AVG(REC.calc::inDefined), AVG(REC.calc::inActivated), AVG(REC.calc::inSent), AVG(REC.calc::inStarting), AVG(REC.calc::inRunning), AVG(REC.calc::inHolding);
dump avints;

--SKIPS = filter REC by calc::SKIP==1 and TRANSFERTYPE=='fax';
--dump SKIPS;

WAITH = filter REC by calc::SKIP!=0 and TRANSFERTYPE is null;
WAITB = foreach WAITH generate OWAIT, OWAIT/300 as bin;
WG = group WAITB by bin;
WH = foreach WG generate group, COUNT(WAITB.bin);
dump WH;



-- personal jars - newest versions
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-mapred-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-tools-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-core-2.4.2.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-databind-2.3.1.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/json-simple-1.1.1.jar';


-- PANDAID:long,  JOBDEFINITIONID:long, SCHEDULERID:chararray, PILOTID:chararray, CREATIONTIME:long, CREATIONHOST:chararray,MODIFICATIONTIME:long, MODIFICATIONHOST:chararray, ATLASRELEASE:chararray, TRANSFORMATION:chararray, HOMEPACKAGE:chararray, PRODSERIESLABEL:chararray, PRODSOURCELABEL:chararray, PRODUSERID:chararray, ASSIGNEDPRIORITY:long, CURRENTPRIORITY:long, ATTEMPTNR:int, MAXATTEMPT:int, JOBSTATUS:chararray, JOBNAME:chararray, MAXCPUCOUNT:long, MAXCPUUNIT:chararray, MAXDISKCOUNT:long, MAXDISKUNIT:chararray, IPCONNECTIVITY:chararray, MINRAMCOUNT:long, MINRAMUNIT:chararray, STARTTIME:long, ENDTIME:long, CPUCONSUMPTIONTIME:long, CPUCONSUMPTIONUNIT:chararray, COMMANDTOPILOT:chararray, TRANSEXITCODE:chararray, PILOTERRORCODE:int, PILOTERRORDIAG:chararray, EXEERRORCODE:int, EXEERRORDIAG:chararray, SUPERRORCODE:int, SUPERRORDIAG:chararray, DDMERRORCODE:int, DDMERRORDIAG:chararray, BROKERAGEERRORCODE:int, BROKERAGEERRORDIAG:chararray, JOBDISPATCHERERRORCODE:int, JOBDISPATCHERERRORDIAG:chararray, TASKBUFFERERRORCODE:int, TASKBUFFERERRORDIAG:chararray, COMPUTINGSITE:chararray, COMPUTINGELEMENT:chararray, PRODDBLOCK:chararray, DISPATCHDBLOCK:chararray, DESTINATIONDBLOCK:chararray, DESTINATIONSE:chararray, NEVENTS:long, GRID:chararray, CLOUD:chararray, CPUCONVERSION:float, SOURCESITE:chararray, DESTINATIONSITE:chararray, TRANSFERTYPE:chararray, TASKID:long, CMTCONFIG:chararray, STATECHANGETIME:long, PRODDBUPDATETIME:long, LOCKEDBY:chararray, RELOCATIONFLAG:int, JOBEXECUTIONID:long, VO:chararray, PILOTTIMING:chararray, WORKINGGROUP:chararray, PROCESSINGTYPE:chararray, PRODUSERNAME:chararray, NINPUTFILES:int, COUNTRYGROUP:chararray, BATCHID:chararray, PARENTID:long, SPECIALHANDLING:chararray, JOBSETID:long, CORECOUNT:int, NINPUTDATAFILES:int, INPUTFILETYPE:chararray, INPUTFILEPROJECT:chararray, INPUTFILEBYTES:long, NOUTPUTDATAFILES:int,OUTPUTFILEBYTES:long, JOBMETRICS:chararray, WORKQUEUE_ID:int, JEDITASKID:long, JOBSUBSTATUS:chararray,ACTUALCORECOUNT:int