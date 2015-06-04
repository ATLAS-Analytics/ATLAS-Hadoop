-- this simple example filters records to select only onese between to dates
-- subtracts values of two columns
-- calculates average of the difference.

-- running in the following way gives much nicer error reports:
-- pig -x mapreduce script.pig

-- output will be in a lot of very small files. to get output localy download it like this
-- hdfs dfs -getmerge waitGperDay.csv waitGperDay.csv

-- if there was previous result, remove it.
rmf waitGperDay.csv; 

-- registering libraries we will need
REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';

-- these are shortcuts
--DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- load data from middle 10 days of May 2015
PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.2015-05/jobs.2015-05-1*' USING AvroStorage();

-- select only users jobs
F1 = filter PAN by PRODSOURCELABEL=='user' ;

-- selects only a few variables
F2 = foreach F1 generate STARTTIME,CREATIONTIME, STARTTIME - CREATIONTIME as DIFF;

-- calculate average of the new variable over all the entries
grouped = group F2 all;
res = foreach grouped generate AVG(F2.DIFF);
dump res;

-- now per day, where days are counted from 2014-01-01.
CLEAN = FILTER PAN BY MODIFICATIONTIME is not null;
F3 = foreach CLEAN generate DaysBetween(ToDate(MODIFICATIONTIME),ToDate(1388534400000L)) as day, STARTTIME,CREATIONTIME, STARTTIME - CREATIONTIME as DIFF;
GperDay = group F3 by day;
waitGperDay = foreach GperDay generate group, AVG(F3.DIFF) as avgwait;
dump waitGperDay;
store waitGperDay into 'waitGperDay.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');

