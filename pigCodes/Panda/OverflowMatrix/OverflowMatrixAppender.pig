-- this code extracts the latest overflow job info and appends it to 
-- results/Panda/OverflowMatrix/Overflows
-- should be executed like this:
-- pig -f OverflowMatrixAppender.pig -p DateToExtract=20150723

REGISTER '/usr/lib/pig/piggybank.jar' ;

-- regular jars
REGISTER '/usr/lib/pig/lib/avro-*.jar';
--REGISTER '/usr/lib/pig/lib/jackson-*.jar';
--REGISTER '/usr/lib/pig/lib/json-*.jar';
--REGISTER '/usr/lib/pig/lib/jython-*.jar';
--REGISTER '/usr/lib/pig/lib/snappy-*.jar';

PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED' USING AvroStorage();

PA = filter PAN by TRANSFERTYPE matches 'fax';

P = foreach PA  { 
    MT=ToDate(MODIFICATIONTIME); 
    ind=INDEXOF(SOURCE,'_',0); 
    GENERATE 
    GetYear(MT)*10000 + GetMonth(MT)*100 + GetDay(MT) as MODT, 
    (long)MODIFICATIONTIME, 
    JOBSTATUS,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, CLOUD, 
    REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, 
    (int)PILOTERRORCODE, (int) NINPUTFILES, 
    ENDTIME - STARTTIME as WALLTIME, 
    STARTTIME - CREATIONTIME as WAITTIME;
    };

JOB = filter P by MODT='$DateToExtract';

store JOB into 'results/Panda/OverflowMatrix/Overflows';


--FJOB = foreach JOBS { MT=ToDate(MODIFICATIONTIME); CT=CurrentTime(); ind=INDEXOF(SOURCE,'_',0); GENERATE GetYear(MT)*10000 + GetMonth(MT)*100 + GetDay(MT) as MODT, JOBSTATUS, (ind>1?SUBSTRING(SOURCE,0,ind):SOURCE) as SOURCESITE, COMPUTINGSITE, PILOTERRORCODE,  DaysBetween(CT,MT) as daysAgo;};

 
 



