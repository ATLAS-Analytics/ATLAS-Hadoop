-- this simple example filters records to select only not HC jobs
-- counts number of jobs per user, per cloud

-- running in the following way gives much nicer error reports:
-- pig -x mapreduce script.pig

-- output will be in a lot of very small files. to get output localy download it like this
-- hdfs dfs -getmerge JobsPerUserPerCloud.csv JobsPerUserPerCloud.csv

-- if there was previous result, remove it.
rmf JobsPerUserPerCloud.csv; 

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

-- clean HC jobs
CLEAN = FILTER PAN BY NOT PRODUSERNAME=='gangarbt';
F3 = foreach CLEAN generate PRODUSERNAME, PANDAID, CLOUD;

GperUserCloud = group F3 by (PRODUSERNAME, CLOUD);
JobsPerUserPerCloud = foreach GperUserCloud generate group, COUNT(F3.PANDAID);
dump JobsPerUserPerCloud;
store JobsPerUserPerCloud into 'JobsPerUserPerCloud.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');




