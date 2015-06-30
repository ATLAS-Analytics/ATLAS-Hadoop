-- remove the intermediate result directory 
rmf /atlas/analytics/intermediate/IO/Reshuffle

-- this code reads from /user/rucio01/nongrid_traces/,  /atlas/analytics/xAODcollector/test.json
-- parses json, calculates readsize averages and
-- stores result for further processing.

REGISTER xAODparser-*.jar
REGISTER json.jar

RECS = LOAD '/atlas/analytics/xAODcollector/test.json'  using PigStorage as (Rec:chararray);
--dump RECS;

B = FOREACH RECS GENERATE xAODparser.Parser(Rec);
describe B;
dump B;
