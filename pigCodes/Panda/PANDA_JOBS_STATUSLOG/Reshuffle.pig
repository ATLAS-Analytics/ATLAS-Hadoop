-- remove the intermediate result directory 
rmf /atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Reshuffle

-- this code reads from /atlas/analytics/panda/JOBS_STATUSLOG, 
-- reshuffles records so that each pandaid has only one row,
-- stores result for further processing.
-- goes from 1 226 804 123 to 242 960 641 records
-- goes to 155 375 067 after 2014/01/01 cut

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;

--PANDAID:long, MODIFICATIONTIME:chararray, JOBSTATUS:chararray, PRODSOURCELABEL:chararray, CLOUD:chararray, COMPUTINGSITE:chararray

AJOBS = LOAD '/atlas/analytics/panda/JOBS_STATUSLOG/2015-03/' USING AvroStorage();

JOBS = filter AJOBS by COMPUTINGSITE=='ANALY_MWT2_SL6' and MODIFICATIONTIME>1388534400000L; // 2014.1.1

grJ = group JOBS by (PANDAID,COMPUTINGSITE,PRODSOURCELABEL);
gJOBS = foreach grJ { generate FLATTEN(group) as (PANDAID,COMPUTINGSITE,PRODSOURCELABEL), myfuncs.BagToBag(JOBS); };
STORE gJOBS into '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Reshuffle';
