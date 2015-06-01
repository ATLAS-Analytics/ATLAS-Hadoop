REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://atlas-analytics-es.cern.ch:9200');

JOBS = LOAD '/atlas/analytics/panda/intermediate/JOBS_STATUSLOG/Intervals' as (CRTIME: long, PANDAID:long, CLOUD:chararray, COMPUTINGSITE:chararray, PRODSOURCELABEL:chararray, times:bag{tuple(state:chararray, time:long)}, SKIPPED:int, SORTED:int);
L = LIMIT JOBS 100; dump L;

STORE L INTO 'interval-data-2015-test/interval_record' USING EsStorage();


