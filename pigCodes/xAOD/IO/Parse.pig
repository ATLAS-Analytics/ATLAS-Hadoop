-- this code reads from /user/rucio01/nongrid_traces/
-- parses json, stores it into AVRO. 
-- except it does not work due to some Avrostorage stupidity.

rmf /atlas/analytics/xAODcollector/xAODparsedData

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER xAODparser-*.jar
REGISTER json.jar
REGISTER '/usr/lib/pig/lib/avro-*.jar';

RECS = LOAD '/atlas/analytics/xAODcollector/2015-09-10.json'  using PigStorage as (Rec:chararray);
describe RECS;
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec)) as (PandaID: long, TaskID: long, IP: chararray, ROOT_RELEASE: chararray, ReadCalls: long, ReadSize: long, CacheSize: long, accessedFiles: (name: chararray), AccessedBranches: map[int], AccessedContainers: map[int], fileType: chararray, storageType: chararray);
describe B;
-- dump B;

STORE B INTO '/atlas/analytics/xAODcollector/xAODparsedData' USING AvroStorage();
