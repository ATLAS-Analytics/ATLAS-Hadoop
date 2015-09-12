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

D = foreach F generate line::AccessedBranches as AB,  line::fileType as FT ; -- line::AccessedContainers,


-- ******************** GROUPING per input file type*******************

G = GROUP D by FT;
S = FOREACH G GENERATE group, COUNT(D), FLATTEN(xAODparser.HeatMapCounts(D.AB));

describe S;
-- dump S;

-- S1 = FOREACH G GENERATE group, COUNT(D), FLATTEN(xAODparser.HeatMap(D.AB));

STORE S INTO 'heatmapJobs.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
--STORE S1 INTO 'heatmapEvents.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
