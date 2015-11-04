-- remove results directory 
rmf results/Panda/RucioInfluence/
-- this code derives how long a job stays in different states.
-- job states are described here: https://twiki.cern.ch/twiki/bin/view/PanDA/PandaShiftGuide#Job_state_definitions_in_Panda

register '/usr/lib/pig/piggybank.jar' ;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

Register 'myudfs.py' using jython as myfuncs;

RAWJOBS = LOAD '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Reshuffle' as (PANDAID:long, CLOUD:chararray, COMPUTINGSITE:chararray, PRODSOURCELABEL:chararray, times:bag{tuple(state:chararray, time:long)});

JOBS = filter RAWJOBS by PRODSOURCELABEL=='managed'; 

recJOBS = foreach JOBS generate PANDAID,CLOUD,COMPUTINGSITE,myfuncs.HoldingToTransferringTimes(times) as res;

--A = LIMIT recJOBS 1000; dump A;


-- ******** show counts per ERROR code *******
grPerRes = group recJOBS by res.SKIP;
B = foreach grPerRes generate group, COUNT(recJOBS.PANDAID);
dump B;


-- ******** select only jobs that have 0 error code *********
frecJOBS = filter recJOBS by res.SKIP==0;

-- ******** add Excel readable date ***********
AD = FOREACH frecJOBS {	
    MT=UnixToISO(res.LASTMODIFIED);
	GENERATE 
	SUBSTRING(MT,0,10) as MDT:chararray, res.RESULT as RESULTinS, CLOUD, COMPUTINGSITE;
	};


-- ********* calculate averages per day ***********
ADg = GROUP AD BY MDT;
AvgResTimePerDay = FOREACH ADg GENERATE group, AVG(AD.RESULTinS) as AvgDT;
DESCRIBE AvgResTimePerDay;
STORE AvgResTimePerDay into 'results/Panda/RucioInfluence/AvgTimeInWaitingPerDay.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');


/*
-- ********* dumps a histogram of values for one date ********
oneDay = FILTER AD by MDT=='2015-01-07';
forHist = FOREACH oneDay generate MDT, CLOUD, COMPUTINGSITE, RESULTinS * $BIN_COUNT / ($MAX + 1) as bin_id; 
hg = GROUP forHist by bin_id;
histo = FOREACH hg generate group, COUNT(forHist.CLOUD);
dump histo; 
*/

--STORE recJOBS into 'results/Panda/RucioInfluence/RecalculatedTimes';
--dump recJOBS;

--grJ = group JOBS by JOBSTATUS;
--nEntriesPerState = foreach grJ generate group, COUNT(JOBS.PANDAID);
--dump nEntriesPerState;
--grJ = group JOBS by PRODSOURCELABEL;
--nEntriesPerPRODSOURCELABEL = foreach grJ generate group, COUNT(JOBS.PANDAID);
--dump nEntriesPerPRODSOURCELABEL;  


/*

split FJOBS into 
failed if JOBSTATUS=='failed',
cancelled if JOBSTATUS=='cancelled',
assigned if JOBSTATUS=='assigned',
pending if JOBSTATUS=='pending',
activated if JOBSTATUS=='activated',
sent if JOBSTATUS=='sent',
defined if JOBSTATUS=='defined',
holding if JOBSTATUS=='holding',
merging if JOBSTATUS=='merging',
finished if JOBSTATUS=='finished',
starting if JOBSTATUS=='starting',
transferring if JOBSTATUS=='transferring',
waiting if JOBSTATUS=='waiting',
running if JOBSTATUS=='running';


JoinedJobs = JOIN assigned BY PANDAID,waiting BY PANDAID;

JJ = FOREACH JoinedJobs GENERATE 
	waiting::PANDAID, 
	waiting::MODIFICATIONTIME as TIME1, 
	assigned::MODIFICATIONTIME as TIME2,
	assigned::PRODSOURCELABEL,
	assigned::CLOUD,
	assigned::COMPUTINGSITE;

AD = FOREACH JJ {
	d1 = ToDate(TIME1, 'yyyy-MM-dd HH:mm:ss.0');
	d2 = ToDate(TIME2, 'yyyy-MM-dd HH:mm:ss.0'); 
	d3 = ToDate(1388534400000L);
	GENERATE 
	DaysBetween(d1,d3) as day, PANDAID, SecondsBetween(d2,d1) as DT, PRODSOURCELABEL, CLOUD, COMPUTINGSITE;
	};

T = FILTER AD BY DT<0;

ADg = GROUP AD BY day;
AvgTimeInWaitingPerDay = FOREACH ADg GENERATE group, AVG(AD.DT) as AvgDT;
STORE AvgTimeInWaitingPerDay into 'results/Panda/RucioInfluence/AvgTimeInWaitingPerDay.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');



JoinedJobs = JOIN transferring BY PANDAID,finished BY PANDAID;

JJ = FOREACH JoinedJobs GENERATE 
	transferring::PANDAID, 
	transferring::MODIFICATIONTIME as TIME1, 
	finished::MODIFICATIONTIME as TIME2,
	finished::PRODSOURCELABEL,
	finished::CLOUD,
	finished::COMPUTINGSITE;

AD = FOREACH JJ {
	d1 = ToDate(TIME1, 'yyyy-MM-dd HH:mm:ss.0');
	d2 = ToDate(TIME2, 'yyyy-MM-dd HH:mm:ss.0'); 
	d3 = ToDate(1388534400000L);
	GENERATE 
	DaysBetween(d1,d3) as day, PANDAID, SecondsBetween(d2,d1) as DT, PRODSOURCELABEL, CLOUD, COMPUTINGSITE;
	};

ADg = GROUP AD BY day;
AvgTimeInTransferringPerDay = FOREACH ADg GENERATE group, AVG(AD.DT) as AvgDT;
STORE AvgTimeInTransferringPerDay into 'results/Panda/RucioInfluence/AvgTimeInTransferringPerDay.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');

(failed,9 101 733)
(cancelled,5 960 739)
(assigned,12 463 703)
(pending,49 636 694)
(activated,57 593 860)
(sent,54 182 267)
(defined,77 515 630)
(holding,53 008 761)
(merging,9 851 272)
(finished,44 036 274)
(starting,54 905 383)
(transferring,29 396 884)
(waiting,504 475)
(running,54 765 958)
*/