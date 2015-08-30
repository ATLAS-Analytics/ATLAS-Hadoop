rmf heatmapcounts.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER xAODparser-*.jar
REGISTER json.jar

-- ****************** TRACES *************************

RECS = LOAD '/atlas/analytics/xAODcollector/2015-08-*.json'  using PigStorage as (Rec:chararray);
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;
-- dump B;


F = filter B BY PandaID > 0L;

D = foreach F generate line::PandaID as PID, SIZE(line::accessedFiles) as AF, line::AccessedBranches as AB, line::AccessedContainers, line::fileType as FT ;

-- ********************** PANDA  ********************


PAN = LOAD '/atlas/analytics/panda/jobs/2015-08-*' USING AvroStorage();
describe PAN;

PA = filter PAN by PRODSOURCELABEL matches 'user' AND NOT PRODUSERNAME matches 'gangarbt';

JO = JOIN PA BY PANDAID, D BY PID;
describe JO;

-- ******************** GROUPING per input file type*******************

G = GROUP JO by PA::INPUTFILETYPE;
S = FOREACH G GENERATE group, COUNT(JO), FLATTEN(xAODparser.HeatMapCounts(JO.D::AB));
describe S;
-- dump S;

STORE S INTO 'heatmapcounts.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
