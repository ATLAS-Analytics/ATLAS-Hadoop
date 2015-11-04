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


PAN = LOAD '/atlas/analytics/panda/jobs/2015-10-*' USING AvroStorage();
DESCRIBE PAN;


PA = filter PAN by JOBSTATUS=='finished' and (COMPUTINGSITE=='ANALY_AGLT2_SL6' or COMPUTINGSITE=='ANALY_MWT2_SL6' or COMPUTINGSITE=='ANALY_BNL_SHORT' or COMPUTINGSITE=='ANALY_SFU' or COMPUTINGSITE=='ANALY_SLAC');

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
