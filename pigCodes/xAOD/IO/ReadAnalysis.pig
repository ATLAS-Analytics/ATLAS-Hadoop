-- this code reads from /user/rucio01/nongrid_traces/
-- parses json, calculates averages of readsize readcalls and cachesize

rmf perFileType.csv
rmf perStorageType.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER xAODparser-*.jar
REGISTER json.jar
REGISTER '/usr/lib/pig/lib/avro-*.jar';


RECS = LOAD '/atlas/analytics/xAODcollector/2015-09-*.json'  using PigStorage as (Rec:chararray);
describe RECS;
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec)) as (PandaID: long, TaskID: long, IP: chararray, ROOT_RELEASE: chararray, ReadCalls: long, ReadSize: long, CacheSize: long, accessedFiles: (name: chararray), AccessedBranches: map[int], AccessedContainers: map[int], fileType: chararray, storageType: chararray);
describe B;
-- dump B;


-- ROOT_VERSIONS popularity
-- ****************************
-- R_GROUP= GROUP B BY ROOT_RELEASE;
-- RCOUNT = FOREACH R_GROUP GENERATE group, COUNT(B);
-- dump RCOUNT;
-- -----------------------------

F = filter B BY PandaID > 0L;

-- when looking at GRID jobs it is important to split user and production jobs
-- ****************************************************************************
-- PAN = LOAD '/atlas/analytics/panda/jobs/2015-09-*' USING AvroStorage();
-- describe PAN;
-- 
-- PA = filter PAN by PRODSOURCELABEL matches 'managed' AND NOT PRODUSERNAME matches 'gangarbt';
-- 
-- R_GROUP= GROUP PA ALL;
-- RCOUNT = FOREACH R_GROUP GENERATE COUNT(PA);
-- dump RCOUNT; 
-- 
-- JO = JOIN PA BY PANDAID, F BY PandaID;
-- describe JO;
-- 
-- R_GROUP= GROUP JO ALL;
-- RCOUNT = FOREACH R_GROUP GENERATE COUNT(JO);
-- dump RCOUNT; 
-- 
-- D = foreach JO generate ReadCalls as RC, ReadSize as RS, CacheSize as CS, SIZE(accessedFiles) as AF, SIZE(AccessedBranches) as AB, SIZE(AccessedContainers) as AC , fileType as FT , storageType as ST;
-- ------------------------------------------------------------------------------------

-- here one needs to fix CacheSize as it has meaning encoded:
-- negative value is number of bytes, positive number is number of events to cache.

D = foreach F generate ReadCalls as RC, ReadSize as RS, CacheSize as CS, SIZE(accessedFiles) as AF, SIZE(AccessedBranches) as AB, SIZE(AccessedContainers) as AC , fileType as FT , storageType as ST;


G = GROUP D by FT;
S = FOREACH G GENERATE group, COUNT(D), AVG(D.RC), AVG(D.RS), AVG(D.CS), AVG(D.AF), AVG(D.AB), AVG(D.AC);
dump S;

STORE S INTO 'perFileType.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');

G1 = GROUP D by ST;
S1 = FOREACH G1 GENERATE group, COUNT(D), AVG(D.RC), AVG(D.RS), AVG(D.CS), AVG(D.AF), AVG(D.AB), AVG(D.AC);
dump S1;

STORE S1 INTO 'perStorageType.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');

