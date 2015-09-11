-- this code reads from /user/rucio01/nongrid_traces/
-- parses json, calculates averages of readsize readcalls and cachesize

rmf perFileType.csv
rmf perStorageType.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER xAODparser-*.jar
REGISTER json.jar

RECS = LOAD '/atlas/analytics/xAODcollector/2015-09-*.json'  using PigStorage as (Rec:chararray);
describe RECS;
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec)), prodsourcelabel;
describe B;
-- dump B;


F = filter B BY PandaID > 0L AND prodsourcelabel == 'user';

-- here one needs to fix CacheSize as it has meaning encoded:
-- negative value is number of bytes, positive number is number of events to cache.

D = foreach F generate line::ReadCalls as RC, line::ReadSize as RS, line::CacheSize as CS, SIZE(line::accessedFiles) as AF, SIZE(line::AccessedBranches) as AB, SIZE(line::AccessedContainers) as AC , line::fileType as FT , line::storageType as ST;


G = GROUP D by FT;
S = FOREACH G GENERATE group, COUNT(D), AVG(D.RC), AVG(D.RS), AVG(D.CS), AVG(D.AF), AVG(D.AB), AVG(D.AC);
dump S;

STORE S INTO 'perFileType.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');

G1 = GROUP D by ST;
S1 = FOREACH G1 GENERATE group, COUNT(D), AVG(D.RC), AVG(D.RS), AVG(D.CS), AVG(D.AF), AVG(D.AB), AVG(D.AC);
dump S1;

STORE S1 INTO 'perStorageType.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');

