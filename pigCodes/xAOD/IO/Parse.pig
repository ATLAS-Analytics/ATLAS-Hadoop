-- this code reads from /user/rucio01/nongrid_traces/
-- parses json, calculates averages of readsize readcalls and cachesize

rmf /atlas/analytics/xAODcollector/xAODparsedData

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER xAODparser-*.jar
REGISTER json.jar
REGISTER '/usr/lib/pig/lib/avro-*.jar';

RECS = LOAD '/atlas/analytics/xAODcollector/2015-09-*.json'  using PigStorage as (Rec:chararray);
describe RECS;
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec)) AS (PandaID: long, TaskID: long, IP: chararray, ROOT_RELEASE: chararray, ReadCalls: long, ReadSize: long, CacheSize: long, accessedFiles: (name: chararray), AccessedBranches: map[], AccessedContainers: map[], fileType: chararray, storageType: chararray);
describe B;

STORE B INTO '/atlas/analytics/xAODcollector/xAODparsedData' USING PigStorage as (PandaID: long, TaskID: long, IP: chararray, ROOT_RELEASE: chararray, ReadCalls: long, ReadSize: long, CacheSize: long, accessedFiles: (name: chararray), AccessedBranches: map[], AccessedContainers: map[], fileType: chararray, storageType: chararray);
--STORE B INTO '/atlas/analytics/xAODcollector/xAODparsedData' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

-- dump B;
