rmf /atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Intervals

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;

JOBS = LOAD '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Reshuffle' as (PANDAID:long, CLOUD:chararray, COMPUTINGSITE:chararray, PRODSOURCELABEL:chararray, times:bag{tuple(state:chararray, time:long)});

JOBInts = foreach JOBS  generate PANDAID, CLOUD, COMPUTINGSITE, PRODSOURCELABEL, myfuncs.AllTheTimes(times), myfuncs.Skipped(times), myfuncs.Sorted(times) ;
--L = LIMIT JOBInts 100; dump L;

STORE JOBInts into '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Intervals';