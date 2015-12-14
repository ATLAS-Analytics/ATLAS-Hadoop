REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

--REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';
REGISTER '/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/elasticsearch-hadoop-pig-2.2.0-beta1.jar'

REGISTER 'myudfs.py' using jython as myfuncs;

SET default_parallel 5;
SET pig.noSplitCombination TRUE;

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://cl-analytics.mwt2.org:9200');


DKC = LOAD '/user/mgubin/mariatable/' USING AvroStorage();
--DESCRIBE DKC;

--L = LIMIT DKC 1000; --dump L;

-- REC = FOREACH PAN GENERATE PANDAID as pandaid, JOBDEFINITIONID as jobdefinitionid, SCHEDULERID as schedulerid, PILOTID as pilotid, REPLACE(CREATIONTIME,' ','T') as creationtime, CREATIONHOST as creationhost, REPLACE(MODIFICATIONTIME,' ','T') as modificationtime, MODIFICATIONHOST as modificationhost, ATLASRELEASE as atlasrelease, TRANSFORMATION as transformation, HOMEPACKAGE as homepackage, PRODSERIESLABEL as prodserieslabel, PRODSOURCELABEL as prodsourcelabel, PRODUSERID as produserid, ASSIGNEDPRIORITY as assignedpriority, CURRENTPRIORITY as currentpriority, ATTEMPTNR as attemptnr, MAXATTEMPT as maxattempt, JOBSTATUS as jobstatus, JOBNAME as jobname, MAXCPUCOUNT as maxcpucount, MAXCPUUNIT as maxcpuunit, MAXDISKCOUNT as maxdiskcount, MAXDISKUNIT as maxdiskunit, IPCONNECTIVITY as ipconnectivity, MINRAMCOUNT as minramcount, MINRAMUNIT as minramunit, REPLACE(STARTTIME,' ','T') as starttime, REPLACE(ENDTIME,' ','T') as endtime, CPUCONSUMPTIONTIME as cpuconsumptiontime, CPUCONSUMPTIONUNIT as cpuconsumptionunit, COMMANDTOPILOT as commandtopilot, TRANSEXITCODE as transexitcode, PILOTERRORCODE as piloterrorcode, PILOTERRORDIAG as piloterrordiag, EXEERRORCODE as exeerrorcode, EXEERRORDIAG as exeerrordiag, SUPERRORCODE as superrorcode, SUPERRORDIAG as superrordiag, DDMERRORCODE as ddmerrorcode, DDMERRORDIAG as ddmerrordiag, BROKERAGEERRORCODE as brokerageerrorcode, BROKERAGEERRORDIAG as brokerageerrordiag, JOBDISPATCHERERRORCODE as jobdispatchererrorcode, JOBDISPATCHERERRORDIAG as jobdispatchererrordiag, TASKBUFFERERRORCODE as taskbuffererrorcode, TASKBUFFERERRORDIAG as taskbuffererrordiag, COMPUTINGSITE as computingsite, COMPUTINGELEMENT as computingelement, PRODDBLOCK as proddblock, DISPATCHDBLOCK as dispatchdblock, DESTINATIONDBLOCK as destinationdblock, DESTINATIONSE as destinationse, NEVENTS as nevents, GRID as grid, CLOUD as cloud, CPUCONVERSION as cpuconversion, SOURCESITE as sourcesite, DESTINATIONSITE as destinationsite, TRANSFERTYPE as transfertype, TASKID as taskid, CMTCONFIG as cmtconfig,REPLACE(STATECHANGETIME,' ','T') as statechangetime, LOCKEDBY as lockedby, RELOCATIONFLAG as relocationflag, JOBEXECUTIONID as jobexecutionid, VO as vo, PILOTTIMING as pilottiming, WORKINGGROUP as workinggroup, PROCESSINGTYPE as processingtype, PRODUSERNAME as produsername, COUNTRYGROUP as countrygroup, BATCHID as batchid, PARENTID as parentid, SPECIALHANDLING as specialhandling, JOBSETID as jobsetid, CORECOUNT as corecount, NINPUTDATAFILES as ninputdatafiles, INPUTFILETYPE as inputfiletype, INPUTFILEPROJECT as inputfileproject, INPUTFILEBYTES as inputfilebytes, NOUTPUTDATAFILES as noutputdatafiles, OUTPUTFILEBYTES as outputfilebytes, JOBMETRICS as jobmetrics, WORKQUEUE_ID as workqueue_id, JEDITASKID as jeditaskid, JOBSUBSTATUS as jobsubstatus, ACTUALCORECOUNT as actualcorecount, REQID as reqid, MAXRSS as maxrss, MAXVMEM as maxvmem, MAXPSS as maxpss, AVGRSS as avgrss, AVGVMEM as avgvmem, AVGSWAP as avgswap, AVGPSS as avgpss, MAXWALLTIME as maxwalltime, FLATTEN(myfuncs.deriveDurationAndCPUeffNEW(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME)) as (wall_time:int, cpu_eff:float, queue_time:int), FLATTEN(myfuncs.deriveTimes(PILOTTIMING)) as (timeGetJob:int, timeStageIn:int, timeExe:int, timeStageOut:int, timeSetup:int);

STORE REC INTO 'dkc/data' USING EsStorage();
