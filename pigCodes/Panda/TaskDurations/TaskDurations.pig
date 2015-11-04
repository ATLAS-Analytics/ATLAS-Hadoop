-- this code should calculate durations of tasks

rmf tmpresult.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';


-- REGISTER '/usr/lib/pig/lib/jackson-*.jar';
-- REGISTER '/usr/lib/pig/lib/json-*.jar';
-- REGISTER '/usr/lib/pig/lib/snappy-*.jar';

ajobs = LOAD '/atlas/analytics/panda/jobs/2015-10-2*' USING AvroStorage();
DESCRIBE ajobs;

pjobs = filter ajobs by PRODSOURCELABEL=='user' OR PRODSOURCELABEL=='managed' AND ((long)TASKID)>1000;

sjobs = FOREACH pjobs GENERATE CREATIONTIME/1000 as creation_time, STARTTIME/1000 as start_time, ENDTIME/1000 as end_time, (STARTTIME - CREATIONTIME)/1000 as wait_time, (long)PANDAID, (long)TASKID, (ENDTIME-STARTTIME)/1000 as jobs_duration, PRODSOURCELABEL;
DESCRIBE sjobs;

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
