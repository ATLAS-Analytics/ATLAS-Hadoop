-- remove results directory 
rmf results/Panda/statesFlow/
-- this code derives how long a job stays in different states.
-- job states are described here: https://twiki.cern.ch/twiki/bin/view/PanDA/PandaShiftGuide#Job_state_definitions_in_Panda

register '/usr/lib/pig/piggybank.jar' ;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

Register 'myudfs.py' using jython as myfuncs;

RAWJOBS = LOAD '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/ReshuffleFlow' as (PANDAID:long, COMPUTINGSITE:chararray, PRODSOURCELABEL:chararray, STATES:bag{(state:chararray)});

--CJOBS = filter RAWJOBS by PRODSOURCELABEL=='managed'; 

JOBS = foreach RAWJOBS generate COMPUTINGSITE, PANDAID, PRODSOURCELABEL, BagToString(STATES,' ') as path:chararray;

SAMP = filter JOBS by path=='pending defined activated';
L = LIMIT SAMP 100;
dump L;

grPerRes = group JOBS by path;
PATHS = foreach grPerRes generate group, COUNT(JOBS.COMPUTINGSITE) as appearances:long;
STORE PATHS into 'results/Panda/statesFlow/PATHS-managed.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');

FREQUENT = filter PATHS by appearances>1000;
dump FREQUENT;



/*
-- ******** show counts per flow *******
grPerRes = group JOBS by STATES;
B = foreach grPerRes generate group, COUNT(JOBS.PANDAID);
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



-- ********* dumps a histogram of values for one date ********
oneDay = FILTER AD by MDT=='2015-01-07';
forHist = FOREACH oneDay generate MDT, CLOUD, COMPUTINGSITE, RESULTinS * $BIN_COUNT / ($MAX + 1) as bin_id; 
hg = GROUP forHist by bin_id;
histo = FOREACH hg generate group, COUNT(forHist.CLOUD);
dump histo; 

--STORE recJOBS into 'results/Panda/RucioInfluence/RecalculatedTimes';
--dump recJOBS;

--grJ = group JOBS by JOBSTATUS;
--nEntriesPerState = foreach grJ generate group, COUNT(JOBS.PANDAID);
--dump nEntriesPerState;
--grJ = group JOBS by PRODSOURCELABEL;
--nEntriesPerPRODSOURCELABEL = foreach grJ generate group, COUNT(JOBS.PANDAID);
--dump nEntriesPerPRODSOURCELABEL;  


*/