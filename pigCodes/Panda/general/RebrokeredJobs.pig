-- this code select jobs in interval given by the input directory that run in certain ANALY queues and calculates:
-- total job counts
-- average duration of job (from the moment of submission untill the end)
-- average time till running (from the moment of submission)
-- all of it grouped by TransferType, ComputingSite and JobsStatus

--select computingsite,count(*) from atlas_panda.jobsarchived4 where prodsourcelabel='managed' and  taskBufferErrorDiag like '%JEDI%' and jobstatus='cancelled' and taskBufferErrorCode=100 and modificationtime>sysdate-12/24 group by computingsite;
-- rebrokered jobs have taskbuffererrordiag='reassigned by JEDI'

REGISTER '/usr/lib/pig/piggybank.jar' ;

-- regular jars
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';


PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.2015-03-*' USING AvroStorage();
DESCRIBE PAN;


PA = filter PAN by JOBSTATUS=='cancelled' and TASKBUFFERERRORCODE=='100' and (COMPUTINGSITE=='ANALY_AGLT2_SL6' or COMPUTINGSITE=='ANALY_MWT2_SL6' or COMPUTINGSITE=='ANALY_BNL_SHORT' or COMPUTINGSITE=='ANALY_SFU' or COMPUTINGSITE=='ANALY_SLAC');

--PA = filter PAN by JOBSTATUS=='cancelled' and TASKBUFFERERRORCODE=='100' and  TASKBUFFERERRORDIAG matches '.*JEDI.*' and (COMPUTINGSITE=='ANALY_AGLT2_SL6' or COMPUTINGSITE=='ANALY_MWT2_SL6' or COMPUTINGSITE=='ANALY_BNL_SHORT' or COMPUTINGSITE=='ANALY_SFU' or COMPUTINGSITE=='ANALY_SLAC');

P = foreach PA { generate (long)PANDAID,TASKBUFFERERRORDIAG, TRANSFERTYPE,(long)MODIFICATIONTIME,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, CLOUD, REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, (int)PILOTERRORCODE, (int) NINPUTFILES, ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME, FLATTEN(STRSPLIT(PILOTTIMING, '\\u007C')) as (timeGetJob:chararray, timeStageIn:chararray,timeExe:chararray,timeStageOut:chararray,timeCleanUp:chararray); };


jobEffGroup = GROUP P BY (TRANSFERTYPE,COMPUTINGSITE);
jeg = FOREACH jobEffGroup GENERATE group, COUNT(P.MODIFICATIONTIME), AVG(P.WALLTIME)/1000, AVG(P.WAITTIME)/1000;
dump jeg;

jobErrGroup = GROUP P BY (TRANSFERTYPE,TASKBUFFERERRORDIAG);
jerg = FOREACH jobErrGroup GENERATE group, COUNT(P.MODIFICATIONTIME), AVG(P.WALLTIME)/1000, AVG(P.WAITTIME)/1000;
dump jerg;

P1 = foreach P generate PANDAID,TRANSFERTYPE,COMPUTINGSITE, SOURCE, (int)timeGetJob, (int) timeStageIn,(int)timeExe,(int)timeStageOut,(int)timeCleanUp;

jobTimeGroup = GROUP P1 BY (TRANSFERTYPE,COMPUTINGSITE, SOURCE );
jtg = FOREACH jobTimeGroup GENERATE group, AVG(P1.timeGetJob),AVG(P1.timeStageIn),AVG(P1.timeExe),AVG(P1.timeStageOut),AVG(P1.timeCleanUp);
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