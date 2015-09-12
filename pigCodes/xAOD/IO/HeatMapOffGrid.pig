rmf heatmapEvents.csv
rmf heatmapJobs.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER xAODparser-*.jar
REGISTER json.jar

-- ****************** TRACES *************************

RECS = LOAD '/atlas/analytics/xAODcollector/2015-*.json'  using PigStorage as (Rec:chararray);
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;
-- dump B;


F = filter B BY PandaID == 0L;

D = foreach F generate line::PandaID as PID, SIZE(line::accessedFiles) as AF, line::AccessedBranches as AB, line::AccessedContainers, line::fileType as FT ;


-- ******************** GROUPING per input file type*******************

G = GROUP D by FT;
S = FOREACH G GENERATE group, COUNT(D), FLATTEN(xAODparser.HeatMap(AB));
describe S;
-- dump S;

S1 = FOREACH G GENERATE group, COUNT(D), FLATTEN(xAODparser.HeatMapCounts(AB));

STORE S INTO 'heatmapEvents.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
STORE S1 INTO 'heatmapJobs.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
