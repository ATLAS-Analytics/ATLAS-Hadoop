REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;

--define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://atlas-analytics-es.cern.ch:9200');
define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://aianalytics01.cern.ch:9200');

--JOBS = LOAD '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Intervals/part-m-00189' as (timestamp: long, PANDAID:long, CLOUD:chararray, COMPUTINGSITE:chararray, PRODSOURCELABEL:chararray, times:bag{tuple(state:chararray, time:long)}, SKIPPED:int, SORTED:int);

JOBS = LOAD '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Reshuffle/part-r-0003*' as (PANDAID:long, CLOUD:chararray, COMPUTINGSITE:chararray, PRODSOURCELABEL:chararray, times:bag{tuple(state:chararray, time:long)});

JOBInts = foreach JOBS  generate myfuncs.Tstamp(times) as timestamp, PANDAID, CLOUD, COMPUTINGSITE, PRODSOURCELABEL, myfuncs.TheTimes(times) as AT, myfuncs.Skipped(times) as SKIPPED, myfuncs.Sorted(times) as SORTED;

--dump JOBS;
L = foreach JOBInts generate timestamp, PANDAID, CLOUD, COMPUTINGSITE, PRODSOURCELABEL, FLATTEN(AT), SKIPPED, SORTED;
--dump L;
--L = LIMIT JOBS 10000; dump L;

STORE L INTO 'interval-data/interval' USING EsStorage();
