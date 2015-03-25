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


PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.2015-03-1*' USING AvroStorage();
DESCRIBE PAN;


PA = filter PAN by COMPUTINGSITE=='ANALY_AGLT2_SL6' or COMPUTINGSITE=='ANALY_MWT2_SL6' or COMPUTINGSITE=='ANALY_BNL_SHORT' or COMPUTINGSITE=='ANALY_BNL_LONG' or COMPUTINGSITE=='ANALY_SFU' or COMPUTINGSITE=='ANALY_SLAC';

P = foreach PA generate TRANSFERTYPE,(long)MODIFICATIONTIME, JOBSTATUS,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, (int)ATTEMPTNR, REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, (int)PILOTERRORCODE, (long)INPUTFILEBYTES, (long)CPUCONSUMPTIONTIME,  ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME;


jobEffGroup = GROUP P BY (TRANSFERTYPE, COMPUTINGSITE, JOBSTATUS);
jeg = FOREACH jobEffGroup GENERATE group, COUNT(P.MODIFICATIONTIME), AVG(P.WAITTIME)/1000, AVG(P.WALLTIME)/1000, AVG(P.CPUCONSUMPTIONTIME), AVG(P.ATTEMPTNR), SUM(P.INPUTFILEBYTES)/1024/1024/1024 ;
dump jeg;




-- personal jars - newest versions
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-mapred-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-tools-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-core-2.4.2.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-databind-2.3.1.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/json-simple-1.1.1.jar';


-- PANDAID:long,  JOBDEFINITIONID:long, SCHEDULERID:chararray, PILOTID:chararray, CREATIONTIME:long, CREATIONHOST:chararray,MODIFICATIONTIME:long, MODIFICATIONHOST:chararray, ATLASRELEASE:chararray, TRANSFORMATION:chararray, HOMEPACKAGE:chararray, PRODSERIESLABEL:chararray, PRODSOURCELABEL:chararray, PRODUSERID:chararray, ASSIGNEDPRIORITY:long, CURRENTPRIORITY:long, ATTEMPTNR:int, MAXATTEMPT:int, JOBSTATUS:chararray, JOBNAME:chararray, MAXCPUCOUNT:long, MAXCPUUNIT:chararray, MAXDISKCOUNT:long, MAXDISKUNIT:chararray, IPCONNECTIVITY:chararray, MINRAMCOUNT:long, MINRAMUNIT:chararray, STARTTIME:long, ENDTIME:long, CPUCONSUMPTIONTIME:long, CPUCONSUMPTIONUNIT:chararray, COMMANDTOPILOT:chararray, TRANSEXITCODE:chararray, PILOTERRORCODE:int, PILOTERRORDIAG:chararray, EXEERRORCODE:int, EXEERRORDIAG:chararray, SUPERRORCODE:int, SUPERRORDIAG:chararray, DDMERRORCODE:int, DDMERRORDIAG:chararray, BROKERAGEERRORCODE:int, BROKERAGEERRORDIAG:chararray, JOBDISPATCHERERRORCODE:int, JOBDISPATCHERERRORDIAG:chararray, TASKBUFFERERRORCODE:int, TASKBUFFERERRORDIAG:chararray, COMPUTINGSITE:chararray, COMPUTINGELEMENT:chararray, PRODDBLOCK:chararray, DISPATCHDBLOCK:chararray, DESTINATIONDBLOCK:chararray, DESTINATIONSE:chararray, NEVENTS:long, GRID:chararray, CLOUD:chararray, CPUCONVERSION:float, SOURCESITE:chararray, DESTINATIONSITE:chararray, TRANSFERTYPE:chararray, TASKID:long, CMTCONFIG:chararray, STATECHANGETIME:long, PRODDBUPDATETIME:long, LOCKEDBY:chararray, RELOCATIONFLAG:int, JOBEXECUTIONID:long, VO:chararray, PILOTTIMING:chararray, WORKINGGROUP:chararray, PROCESSINGTYPE:chararray, PRODUSERNAME:chararray, NINPUTFILES:int, COUNTRYGROUP:chararray, BATCHID:chararray, PARENTID:long, SPECIALHANDLING:chararray, JOBSETID:long, CORECOUNT:int, NINPUTDATAFILES:int, INPUTFILETYPE:chararray, INPUTFILEPROJECT:chararray, INPUTFILEBYTES:long, NOUTPUTDATAFILES:int,OUTPUTFILEBYTES:long, JOBMETRICS:chararray, WORKQUEUE_ID:int, JEDITASKID:long, JOBSUBSTATUS:chararray,ACTUALCORECOUNT:int