--rmf perFileType.csv
--rmf perStorageType.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER xAODparser-*.jar
REGISTER json.jar

-- ****************** TRACES *************************

RECS = LOAD '/atlas/analytics/xAODcollector/2015-08-15.json'  using PigStorage as (Rec:chararray);
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;
-- dump B;


F = filter B BY PandaID > 0L;

D = foreach F generate line::PandaID as PID, SIZE(line::accessedFiles) as AF, line::AccessedBranches as AB, line::AccessedContainers, line::fileType as FT ;

-- ********************** PANDA  ********************


PAN = LOAD '/atlas/analytics/panda/jobs/2015-08-1*' USING AvroStorage();
describe PAN;

PA = filter PAN by PRODSOURCELABEL matches 'user' AND NOT PRODUSERNAME matches 'gangarbt';

JO = JOIN PA BY PANDAID, D BY PID;
describe JO;

-- ******************** GROUPING per input file type*******************

G = GROUP JO by PA::INPUTFILETYPE;
S = FOREACH G GENERATE group, COUNT(JO), xAODparser.HeatMap(JO.D::AB);
describe S;

L = LIMIT S 10;
dump L;

STORE S INTO 'perFileType.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');

G1 = GROUP D by ST;
S1 = FOREACH G1 GENERATE group, COUNT(D), AVG(D.RC), AVG(D.RS), AVG(D.CS), AVG(D.AF), AVG(D.AB), AVG(D.AC);
dump S1;

STORE S1 INTO 'perStorageType.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');

