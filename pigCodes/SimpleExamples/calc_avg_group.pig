-- this simple example filters records to select only user jobs
-- subtracts values of two columns
-- calculates average of the difference.

-- running in the following way gives much nicer error reports:
-- pig -x mapreduce script.pig

-- to customize logging use "-4 log4j.properties" option

-- output will be in a lot of very small files. to get output localy download it like this
-- hdfs dfs -getmerge waitGperDay.csv waitGperDay.csv

-- if there was previous result, remove it.
rmf InputFileInfoPerUser.csv; 

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

-- select only users jobs
F1 = filter PAN by PRODSOURCELABEL=='user' ;

-- print out 10 rows
L = LIMIT F1 10;
dump L;

-- selects only a few variables
F2 = foreach F1 generate STARTTIME,CREATIONTIME, NINPUTDATAFILES, INPUTFILEBYTES, PRODUSERNAME;

-- calculate average number of the input data files and their average size over all the entries
grouped = group F2 all;
res = foreach grouped generate AVG(F2.NINPUTDATAFILES) as Files, AVG(F2.INPUTFILEBYTES)/1024/1024 as Size;
dump res;

-- now the same but per user
GperUser = group F2 by PRODUSERNAME;
InputFileInfoPerUser = foreach GperUser generate group, AVG(F2.NINPUTDATAFILES) as Files, AVG(F2.INPUTFILEBYTES)/1024/1024 as Size;
dump InputFileInfoPerUser;
store InputFileInfoPerUser into 'InputFileInfoPerUser.csv' USING CSVExcelStorage(',', 'NO_MULTILINE');

