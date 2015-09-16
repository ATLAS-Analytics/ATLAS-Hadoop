rmf heatmapEvents.csv
rmf heatmapJobs.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER 'json.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';
REGISTER xAODparser-*.jar

REGISTER '/usr/lib/pig/lib/elasticsearch-hadoop-*.jar';
define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://130.127.133.62:9200','es.mapping.names=CurrentTime:@timestamp’);

SET default_parallel 5;
SET pig.noSplitCombination TRUE;


-- ****************** TRACES *************************

RECS = LOAD '/atlas/analytics/xAODcollector/$INPD.json'  using PigStorage as (Rec:chararray);
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec)) as (PandaID: long, TaskID: long, IP: chararray, ROOT_RELEASE: chararray, ReadCalls: long, ReadSize: long, CacheSize: long, accessedFiles: (name: chararray), AccessedBranches: map[int], AccessedContainers: map[int], fileType: chararray, storageType: chararray);
describe B;
-- dump B;

C = filter B BY PandaID == 0L;

D = foreach C generate AccessedBranches as AB, fileType as FT; 


-- ******************** GROUPING per input file type*******************

E = GROUP D by FT;
S = FOREACH E GENERATE group as FileType, 'OnGrid' as source, CurrentTime() as CurrentTime, COUNT(D) as Jobs, FLATTEN(xAODparser.HeatMapCounts(D.AB));

describe S;
-- dump S;

-- S1 = FOREACH G GENERATE group, COUNT(D), FLATTEN(xAODparser.HeatMap(D.AB));

STORE S INTO 'xaod_monitoring_$INPD/xaod_jobs' USING EsStorage();

