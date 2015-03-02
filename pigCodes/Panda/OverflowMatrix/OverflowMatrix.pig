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


-- PANDAID:long,  JOBDEFINITIONID:long, SCHEDULERID:chararray, PILOTID:chararray, CREATIONTIME:long, CREATIONHOST:chararray,MODIFICATIONTIME:long, MODIFICATIONHOST:chararray, ATLASRELEASE:chararray, TRANSFORMATION:chararray, HOMEPACKAGE:chararray, PRODSERIESLABEL:chararray, PRODSOURCELABEL:chararray, PRODUSERID:chararray, ASSIGNEDPRIORITY:long, CURRENTPRIORITY:long, ATTEMPTNR:int, MAXATTEMPT:int, JOBSTATUS:chararray, JOBNAME:chararray, MAXCPUCOUNT:long, MAXCPUUNIT:chararray, MAXDISKCOUNT:long, MAXDISKUNIT:chararray, IPCONNECTIVITY:chararray, MINRAMCOUNT:long, MINRAMUNIT:chararray, STARTTIME:long, ENDTIME:long, CPUCONSUMPTIONTIME:long, CPUCONSUMPTIONUNIT:chararray, COMMANDTOPILOT:chararray, TRANSEXITCODE:chararray, PILOTERRORCODE:int, PILOTERRORDIAG:chararray, EXEERRORCODE:int, EXEERRORDIAG:chararray, SUPERRORCODE:int, SUPERRORDIAG:chararray, DDMERRORCODE:int, DDMERRORDIAG:chararray, BROKERAGEERRORCODE:int, BROKERAGEERRORDIAG:chararray, JOBDISPATCHERERRORCODE:int, JOBDISPATCHERERRORDIAG:chararray, TASKBUFFERERRORCODE:int, TASKBUFFERERRORDIAG:chararray, COMPUTINGSITE:chararray, COMPUTINGELEMENT:chararray, PRODDBLOCK:chararray, DISPATCHDBLOCK:chararray, DESTINATIONDBLOCK:chararray, DESTINATIONSE:chararray, NEVENTS:long, GRID:chararray, CLOUD:chararray, CPUCONVERSION:float, SOURCESITE:chararray, DESTINATIONSITE:chararray, TRANSFERTYPE:chararray, TASKID:long, CMTCONFIG:chararray, STATECHANGETIME:long, PRODDBUPDATETIME:long, LOCKEDBY:chararray, RELOCATIONFLAG:int, JOBEXECUTIONID:long, VO:chararray, PILOTTIMING:chararray, WORKINGGROUP:chararray, PROCESSINGTYPE:chararray, PRODUSERNAME:chararray, NINPUTFILES:int, COUNTRYGROUP:chararray, BATCHID:chararray, PARENTID:long, SPECIALHANDLING:chararray, JOBSETID:long, CORECOUNT:int, NINPUTDATAFILES:int, INPUTFILETYPE:chararray, INPUTFILEPROJECT:chararray, INPUTFILEBYTES:long, NOUTPUTDATAFILES:int,OUTPUTFILEBYTES:long, JOBMETRICS:chararray, WORKQUEUE_ID:int, JEDITASKID:long, JOBSUBSTATUS:chararray,ACTUALCORECOUNT:int

PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.$INPD' USING AvroStorage();
DESCRIBE PAN;

PA = filter PAN by TRANSFERTYPE matches 'fax';

P = foreach PA generate (long)MODIFICATIONTIME, JOBSTATUS,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, CLOUD, REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, (int)PILOTERRORCODE, (int) NINPUTFILES, ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME;

store P into 'results/Panda/OverflowMatrix/Overflows/jobs.$INPD';


JOBS = LOAD 'results/Panda/OverflowMatrix/Overflows/jobs.$INPD' as (MODIFICATIONTIME:long, JOBSTATUS:chararray,  STARTTIME:long, ENDTIME:long, COMPUTINGSITE:chararray, DESTINATIONSE:chararray, CLOUD:chararray, SOURCE:chararray, DESTINATIONSITE:chararray,  PILOTERRORCODE:int, NINPUTFILES:int, WALLTIME:long, WAITTIME:long);

FJOB = foreach JOBS { MT=ToDate(MODIFICATIONTIME); CT=CurrentTime(); ind=INDEXOF(SOURCE,'_',0); GENERATE GetYear(MT)*10000 + GetMonth(MT)*100 + GetDay(MT) as MODT, JOBSTATUS, (ind>1?SUBSTRING(SOURCE,0,ind):SOURCE) as SOURCESITE, COMPUTINGSITE, PILOTERRORCODE,  DaysBetween(CT,MT) as daysAgo;};

JOB = filter FJOB by daysAgo<7L;

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
 
 


