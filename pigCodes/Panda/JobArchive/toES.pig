REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://aianalytics01.cern.ch:9200');


PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.$INPD/part-m-00000.avro' USING AvroStorage();
DESCRIBE PAN;

--L = LIMIT JOBS 10000; dump L;

STORE PAN INTO 'job_archive/data-$INPD' USING EsStorage();
