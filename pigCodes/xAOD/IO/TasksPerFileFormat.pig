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

F = filter B BY TaskID > 0L;


D = foreach F generate fileType, TaskID;

G = GROUP D by fileType;
-- dump G;
S = FOREACH G {
    D1 = D.TaskID;
    D2 = distinct D1;
    GENERATE group, COUNT(D2);
    }
dump S;

