-- this simple example filters records to select only onese between two dates
-- subtracts values of two columns
-- calculates average of the difference.

-- running in the following way gives much nicer error reports:
-- pig -x mapreduce script.pig

-- output will be in a lot of very small files. to get output localy download it like this
-- hdfs dfs -getmerge waitGperDay.csv waitGperDay.csv

-- if there was previous result, remove it.
rmf JobsPerDay.csv; 
rmf JobsForTheDay.csv;

-- registering libraries we will need
REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';

-- these are shortcuts
--DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- load data from middle 10 days of May 2015
PAN = LOAD '/atlas/analytics/panda/jobs/2015-05-1*' USING AvroStorage();

-- to see all the columns defined.
DESCRIBE PAN;

-- grouping per day 
CLEAN = FILTER PAN BY MODIFICATIONTIME is not null;
F3 = foreach CLEAN generate DaysBetween(ToDate(MODIFICATIONTIME),ToDate(1388534400000L)) as day, PANDAID, MODIFICATIONTIME as MT;

GperDay = group F3 by day;
JobsPerDay = foreach GperDay generate group, COUNT(F3.MT);
dump JobsPerDay;
store JobsPerDay into 'JobsPerDay.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');

-- F4 = FILTER F3 by day==138;
-- dump F4.PANDAID;
-- store F4 into 'JobsForTheDay.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');



