REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;

SET default_parallel 5;
SET pig.noSplitCombination TRUE;

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://130.127.133.62:9200');


PAN = LOAD '/atlas/analytics/panda/jobs/$INPD' USING AvroStorage();
--DESCRIBE PAN;

--L = LIMIT PAN 1000; --dump L;

REC = FOREACH PAN GENERATE PANDAID,JOBDEFINITIONID,SCHEDULERID,PILOTID,REPLACE(CREATIONTIME,' ','T') as creationtime,CREATIONHOST,REPLACE(MODIFICATIONTIME,' ','T') as modificationtime,MODIFICATIONHOST,ATLASRELEASE,TRANSFORMATION,HOMEPACKAGE,PRODSERIESLABEL,PRODSOURCELABEL,PRODUSERID,ASSIGNEDPRIORITY,CURRENTPRIORITY,ATTEMPTNR,MAXATTEMPT,JOBSTATUS,JOBNAME,MAXCPUCOUNT,MAXCPUUNIT,MAXDISKCOUNT,MAXDISKUNIT,IPCONNECTIVITY,MINRAMCOUNT,MINRAMUNIT,REPLACE(STARTTIME,' ','T') as starttime,REPLACE(ENDTIME,' ','T') as endtime,CPUCONSUMPTIONTIME,CPUCONSUMPTIONUNIT,COMMANDTOPILOT,TRANSEXITCODE,PILOTERRORCODE,PILOTERRORDIAG,EXEERRORCODE,EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG,COMPUTINGSITE,COMPUTINGELEMENT,PRODDBLOCK,DISPATCHDBLOCK,DESTINATIONDBLOCK,DESTINATIONSE,NEVENTS,GRID,CLOUD,CPUCONVERSION,SOURCESITE,DESTINATIONSITE,TRANSFERTYPE,TASKID,CMTCONFIG,REPLACE(STATECHANGETIME,' ','T') as statechangetime,LOCKEDBY,RELOCATIONFLAG,JOBEXECUTIONID,VO,PILOTTIMING,WORKINGGROUP,PROCESSINGTYPE,PRODUSERNAME,COUNTRYGROUP,BATCHID,PARENTID,SPECIALHANDLING,JOBSETID,CORECOUNT,NINPUTDATAFILES,INPUTFILETYPE,INPUTFILEPROJECT,INPUTFILEBYTES,NOUTPUTDATAFILES,OUTPUTFILEBYTES,JOBMETRICS,WORKQUEUE_ID,JEDITASKID,JOBSUBSTATUS,ACTUALCORECOUNT,FLATTEN(myfuncs.deriveDurationAndCPUeffNEW(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME)) as (wall_time:int, cpu_eff:float, queue_time:int), FLATTEN(myfuncs.deriveTimes(PILOTTIMING)) as (timeGetJob:int, timeStageIn:int, timeExe:int, timeStageOut:int, timeSetup:int);

STORE REC INTO 'jobs_archive_$INPD/jobs_data' USING EsStorage();
