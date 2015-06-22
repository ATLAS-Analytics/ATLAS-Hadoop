-- this code should make calculate durations of tasks

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';


-- REGISTER '/usr/lib/pig/lib/jackson-*.jar';
-- REGISTER '/usr/lib/pig/lib/json-*.jar';
-- REGISTER '/usr/lib/pig/lib/snappy-*.jar';

ajobs = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.2015-03' USING AvroStorage();
DESCRIBE ajobs;

pjobs = filter ajobs by PRODSOURCELABEL=='user' OR PRODSOURCELABEL=='managed' AND TASKID:>1000;

sjobs = FOREACH pjobs GENERATE CREATIONTIME/1000 as creation_time, STARTTIME/1000 as start_time, ENDTIME/1000 as end_time, (STARTTIME - CREATIONTIME)/1000 as wait_time, PANDAID, TASKID, (ENDTIME-STARTTIME)/1000 as jobs_duration, PRODSOURCELABEL;

jGroup = GROUP sjobs by TASKID;
tasks = FOREACH jGroup GENERATE group, COUNT(sjobs.PANDAID), MAX(sjobs.end_time)-MIN(sjobs.creation_time) as task_duration, AVG(sjobs.jobs_duration) as avg_job_duration, AVG(sjobs.wait_time) as avg_wait_time;
DUMP tasks;

STORE tasks INTO 'tmpresult.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');

task
--CLOUD

P = foreach PA { generate (long)PANDAID,TRANSFORMATION,JOBNAME,COMMANDTOPILOT, TRANSFERTYPE,(long)MODIFICATIONTIME,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, CLOUD, REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, (int)PILOTERRORCODE, (int) NINPUTFILES, ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME, FLATTEN(STRSPLIT(PILOTTIMING, '\\u007C')) as (timeGetJob:chararray, timeStageIn:chararray,timeExe:chararray,timeStageOut:chararray,timeCleanUp:chararray); };


jobEffGroup = GROUP P BY (TRANSFERTYPE,COMPUTINGSITE, SOURCE);
jeg = FOREACH jobEffGroup GENERATE group, COUNT(P.MODIFICATIONTIME), AVG(P.WALLTIME)/1000, AVG(P.WAITTIME)/1000;
dump jeg;


P1 = foreach P generate PANDAID,TRANSFORMATION,JOBNAME,COMMANDTOPILOT,TRANSFERTYPE,COMPUTINGSITE, SOURCE, (int)timeGetJob, (int) timeStageIn,(int)timeExe,(int)timeStageOut,(int)timeCleanUp;

jobTimeGroup = GROUP P1 BY (TRANSFERTYPE,COMPUTINGSITE, SOURCE );
jtg = FOREACH jobTimeGroup GENERATE group, TRANSFORMATION,JOBNAME,COMMANDTOPILOT,AVG(P1.timeGetJob),AVG(P1.timeStageIn),AVG(P1.timeExe),AVG(P1.timeStageOut),AVG(P1.timeCleanUp);
dump jtg;

SP = filter P1 by COMPUTINGSITE=='ANALY_MWT2_SL6' and SOURCE=='SLAC';
dump SP;

-- personal jars - newest versions
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-mapred-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-tools-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-core-2.4.2.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-databind-2.3.1.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/json-simple-1.1.1.jar';


-- PANDAID:long,  JOBDEFINITIONID:long, SCHEDULERID:chararray, PILOTID:chararray, CREATIONTIME:long, CREATIONHOST:chararray,MODIFICATIONTIME:long, MODIFICATIONHOST:chararray, ATLASRELEASE:chararray, TRANSFORMATION:chararray, HOMEPACKAGE:chararray, PRODSERIESLABEL:chararray, PRODSOURCELABEL:chararray, PRODUSERID:chararray, ASSIGNEDPRIORITY:long, CURRENTPRIORITY:long, ATTEMPTNR:int, MAXATTEMPT:int, JOBSTATUS:chararray, JOBNAME:chararray, MAXCPUCOUNT:long, MAXCPUUNIT:chararray, MAXDISKCOUNT:long, MAXDISKUNIT:chararray, IPCONNECTIVITY:chararray, MINRAMCOUNT:long, MINRAMUNIT:chararray, STARTTIME:long, ENDTIME:long, CPUCONSUMPTIONTIME:long, CPUCONSUMPTIONUNIT:chararray, COMMANDTOPILOT:chararray, TRANSEXITCODE:chararray, PILOTERRORCODE:int, PILOTERRORDIAG:chararray, EXEERRORCODE:int, EXEERRORDIAG:chararray, SUPERRORCODE:int, SUPERRORDIAG:chararray, DDMERRORCODE:int, DDMERRORDIAG:chararray, BROKERAGEERRORCODE:int, BROKERAGEERRORDIAG:chararray, JOBDISPATCHERERRORCODE:int, JOBDISPATCHERERRORDIAG:chararray, TASKBUFFERERRORCODE:int, TASKBUFFERERRORDIAG:chararray, COMPUTINGSITE:chararray, COMPUTINGELEMENT:chararray, PRODDBLOCK:chararray, DISPATCHDBLOCK:chararray, DESTINATIONDBLOCK:chararray, DESTINATIONSE:chararray, NEVENTS:long, GRID:chararray, CLOUD:chararray, CPUCONVERSION:float, SOURCESITE:chararray, DESTINATIONSITE:chararray, TRANSFERTYPE:chararray, TASKID:long, CMTCONFIG:chararray, STATECHANGETIME:long, PRODDBUPDATETIME:long, LOCKEDBY:chararray, RELOCATIONFLAG:int, JOBEXECUTIONID:long, VO:chararray, PILOTTIMING:chararray, WORKINGGROUP:chararray, PROCESSINGTYPE:chararray, PRODUSERNAME:chararray, NINPUTFILES:int, COUNTRYGROUP:chararray, BATCHID:chararray, PARENTID:long, SPECIALHANDLING:chararray, JOBSETID:long, CORECOUNT:int, NINPUTDATAFILES:int, INPUTFILETYPE:chararray, INPUTFILEPROJECT:chararray, INPUTFILEBYTES:long, NOUTPUTDATAFILES:int,OUTPUTFILEBYTES:long, JOBMETRICS:chararray, WORKQUEUE_ID:int, JEDITASKID:long, JOBSUBSTATUS:chararray,ACTUALCORECOUNT:int