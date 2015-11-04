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


PAN = LOAD '/atlas/analytics/panda/jobs/2015-10-*' USING AvroStorage();
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

