REGISTER elephant-bird-core-4.9.jar;
REGISTER elephant-bird-pig-4.9.jar;
REGISTER /usr/lib/pig/lib/avro.jar;
REGISTER /usr/lib/pig/piggybank.jar;
REGISTER /usr/lib/avro/avro-mapred.jar;
REGISTER /usr/lib/pig/lib/json-simple-1.1.jar;
REGISTER '/usr/lib/pig/lib/jython-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;


DEFINE decode_json com.twitter.elephantbird.pig.piggybank.JsonStringToMap();

-- message = LOAD '/user/rucio01/dumps/2017-02-02/messages_history' USING AvroStorage();
message = LOAD '/user/rucio01/dumps/$ININD/messages_history' USING AvroStorage();

filt_msg = FILTER message by EVENT_TYPE=='transfer-done';
-- L = LIMIT filt_msg 1000;
-- dump L;

dec_msg = FOREACH filt_msg GENERATE decode_json(PAYLOAD)  as payload;

rdec_msg = FOREACH dec_msg GENERATE myfuncs.checkDate(payload); 


filtered = FILTER rdec_msg by started_at==1;

group_all = GROUP filtered All;
dcount = foreach group_all Generate COUNT(filtered.started_at);
dump dcount;
