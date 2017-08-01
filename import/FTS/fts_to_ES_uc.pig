REGISTER elephant-bird-core-4.9.jar;
REGISTER elephant-bird-pig-4.9.jar;
REGISTER /usr/lib/pig/lib/avro.jar;
REGISTER /usr/lib/pig/piggybank.jar;
REGISTER /usr/lib/avro/avro-mapred.jar;
REGISTER /usr/lib/pig/lib/json-simple-1.1.jar;
REGISTER '/usr/lib/pig/lib/jython-*.jar';

REGISTER '/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/elasticsearch-hadoop-5.1.2/dist/elasticsearch-hadoop-pig-5.1.2.jar'
REGISTER 'myudfs.py' using jython as myfuncs;

SET default_parallel 5;
SET pig.noSplitCombination FALSE;

DEFINE decode_json com.twitter.elephantbird.pig.piggybank.JsonStringToMap();
DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://cl-analytics.mwt2.org:9200','es.mapping.id=request_id','es.batch.size.entries=10000');

message = LOAD '/user/rucio01/dumps/$ININD/messages_history' USING AvroStorage();

filt_msg = FILTER message by EVENT_TYPE=='transfer-done';
-- L = LIMIT filt_msg 10; dump L;

dec_msg = FOREACH filt_msg GENERATE decode_json(PAYLOAD)  as payload;
rdec_msg = FOREACH dec_msg GENERATE FLATTEN(myfuncs.splitPayload(payload)) as (protocol:chararray, dst_rse:chararray, started_at:chararray, scope:chararray, transferred_at:chararray, src_type:chararray, dst_type:chararray, submitted_at:chararray, name:chararray, request_id:chararray, bytes:long, activity:chararray, src_rse:chararray) ;

-- L = LIMIT rdec_msg 100000; dump L;

STORE rdec_msg INTO 'fts_$ININD/transfer-done' USING EsStorage();
