-- remove the intermediate result directory 
rmf /atlas/analytics/panda/intermediate/jobs_status/ReshuffleFlow

-- this code reads from /atlas/analytics/panda/jobs_status, 
-- reshuffles records so that each pandaid has only one row,
-- stores result for further processing.

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;

--PANDAID:long, MODIFICATIONTIME:chararray, JOBSTATUS:chararray, PRODSOURCELABEL:chararray, CLOUD:chararray, COMPUTINGSITE:chararray

JOBS = LOAD '/atlas/analytics/panda/jobs_status/2015-10-2*' USING AvroStorage();

describe JOBS;

grJ = group JOBS by (PANDAID,COMPUTINGSITE,PRODSOURCELABEL);
gJOBS = foreach grJ { generate FLATTEN(group) as (PANDAID,COMPUTINGSITE,PRODSOURCELABEL), myfuncs.OnlyStates(JOBS); };
STORE gJOBS into '/atlas/analytics/panda/intermediate/jobs_status/ReshuffleFlow';
