-- this code simply dumps overflows satifying given criteria. just for debugging the system

REGISTER '/usr/lib/pig/piggybank.jar' ;

-- regular jars
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

JOBS = LOAD 'results/Panda/OverflowMatrix/Overflows' as (MODIFICATIONTIME:long, JOBSTATUS:chararray,  STARTTIME:long, ENDTIME:long, COMPUTINGSITE:chararray, DESTINATIONSE:chararray, CLOUD:chararray, SOURCE:chararray, DESTINATIONSITE:chararray,  PILOTERRORCODE:int, NINPUTFILES:int, WALLTIME:long, WAITTIME:long);

FJOB = foreach JOBS { MT=ToDate(MODIFICATIONTIME); CT=CurrentTime(); ind=INDEXOF(SOURCE,'_',0); GENERATE GetYear(MT)*10000 + GetMonth(MT)*100 + GetDay(MT) as MODT, JOBSTATUS, (ind>1?SUBSTRING(SOURCE,0,ind):SOURCE) as SOURCESITE, COMPUTINGSITE, PILOTERRORCODE,  DaysBetween(CT,MT) as daysAgo;};

JOB = filter FJOB by MODT==20141223L and JOBSTATUS matches 'failed';

gJO = GROUP JOB by (MODT, SOURCESITE, COMPUTINGSITE);
nJobs = FOREACH gJO GENERATE group, COUNT(JOB.MODT);
DUMP nJobs;

-- the same map but split on PILOTERRORCODE
FAILED = filter JOB by PILOTERRORCODE==1099L;
gFJO = GROUP JOB by (MODT, SOURCESITE, COMPUTINGSITE, PILOTERRORCODE);
fJobs = FOREACH gFJO GENERATE group, COUNT(JOB.MODT);
DUMP fJobs;


