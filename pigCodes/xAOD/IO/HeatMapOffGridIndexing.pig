rmf heatmapEvents.csv
rmf heatmapJobs.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';
REGISTER xAODparser-*.jar

REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';
define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://130.127.133.62:9200');

SET default_parallel 5;
SET pig.noSplitCombination TRUE;


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

STORE S INTO 'xaod_branch_usage_$INPD/branch_jobs' USING EsStorage();

