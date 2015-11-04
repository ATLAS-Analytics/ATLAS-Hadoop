-- remove results directory 
rmf results/Panda/OverflowMatrix
-- this code derives overflow matrix that is than uploaded to FSB

REGISTER '/usr/lib/pig/piggybank.jar' ;

-- regular jars
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

-- personal jars - newest versions
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-mapred-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/avro-tools-1.7.7.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-core-2.4.2.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/jackson-databind-2.3.1.jar';
-- REGISTER '/afs/cern.ch/user/i/ivukotic/hadoop/software/json-simple-1.1.1.jar';


PAN = LOAD '/atlas/analytics/panda/jobs/$INPD' USING AvroStorage();
DESCRIBE PAN;

PA = filter PAN by TRANSFERTYPE matches 'fax';

P = foreach PA generate (long)MODIFICATIONTIME, JOBSTATUS,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, CLOUD, REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, (int)PILOTERRORCODE, (int) NINPUTFILES, ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME;

store P into 'results/Panda/OverflowMatrix/Overflows/jobs.$INPD';


JOBS = LOAD 'results/Panda/OverflowMatrix/Overflows/jobs.$INPD' as (MODIFICATIONTIME:long, JOBSTATUS:chararray,  STARTTIME:long, ENDTIME:long, COMPUTINGSITE:chararray, DESTINATIONSE:chararray, CLOUD:chararray, SOURCE:chararray, DESTINATIONSITE:chararray,  PILOTERRORCODE:int, NINPUTFILES:int, WALLTIME:long, WAITTIME:long);

JOB = foreach JOBS { MT=ToDate(MODIFICATIONTIME); ind=INDEXOF(SOURCE,'_',0); GENERATE GetYear(MT)*10000 + GetMonth(MT)*100 + GetDay(MT) as MODT, JOBSTATUS, (ind>1?SUBSTRING(SOURCE,0,ind):SOURCE) as SOURCESITE, COMPUTINGSITE, PILOTERRORCODE;};


gJO = GROUP JOB by (MODT, SOURCESITE, COMPUTINGSITE, JOBSTATUS);
nJobs = FOREACH gJO GENERATE group, COUNT(JOB.MODT);
DUMP nJobs;
store nJobs into 'results/Panda/OverflowMatrix/SourceDestinationStatusCounts';

-- the same map but split on PILOTERRORCODE
FAILED = filter JOB by JOBSTATUS matches 'failed';
gFJO = GROUP JOB by (MODT, SOURCESITE, COMPUTINGSITE, PILOTERRORCODE);
fJobs = FOREACH gFJO GENERATE group, COUNT(JOB.MODT);
DUMP fJobs;
store fJobs into 'results/Panda/OverflowMatrix/SourceDestinationErrorCounts';
 
 


