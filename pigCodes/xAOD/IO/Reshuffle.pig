-- remove the intermediate result directory 
rmf /atlas/analytics/intermediate/IO/Reshuffle

-- this code reads from /user/rucio01/nongrid_traces/,  /atlas/analytics/xAODcollector/test.json
-- parses json, calculates readsize averages and
-- stores result for further processing.

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;


AJOBS = LOAD '/atlas/analytics/xAODcollector/test.json' as (line:chararray); 

JOBS = foreach AJOBS generate myfuncs.decode(AJOBS);

dump JOBS;
