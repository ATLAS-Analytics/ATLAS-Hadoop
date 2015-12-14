REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

--REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';
REGISTER '/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/elasticsearch-hadoop-pig-2.2.0-beta1.jar'

SET default_parallel 5;
SET pig.noSplitCombination TRUE;

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://cl-analytics.mwt2.org:9200');


DKC = LOAD '/user/mgubin/mariatable/' USING AvroStorage();
--DESCRIBE DKC;

--L = LIMIT DKC 1000; --dump L;

STORE DKC INTO 'dkc/data' USING EsStorage();
