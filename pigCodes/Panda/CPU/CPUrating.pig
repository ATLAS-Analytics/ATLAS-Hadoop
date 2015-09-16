REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';

PAN = LOAD '/atlas/analytics/panda/jobs/2015-09-*' USING AvroStorage();
describe PAN;

PA = filter PAN by JEDITASKID>0 AND JOBSTATUS matches 'finished' AND NOT PRODUSERNAME matches 'gangarbt';

P = foreach PA generate JEDITASKID as TaskID, CPUCONSUMPTIONUNIT as CPUtype, CPUCONSUMPTIONTIME as CPUtime;

G = GROUP P by (TaskID, CPUtype);
S = FOREACH G GENERATE FLATTEN(group) AS (TID, CPU), COUNT(P) as jobsInTask, AVG(P.CPUtime) as AvgCPUtime;
describe S;

-- L = LIMIT S 100;
-- dump L;

-- now we should filter out tasks that were executing on one CPU type only.

G1 = GROUP S by TID;
S1 = FOREACH G1 GENERATE group as grTID, COUNT(S) as diffCPUs, SUM(S.jobsInTask) as jobsInTask;
S2 = filter S1 by diffCPUs>1;
  
  
-- join the filtered one with the original 

JO = JOIN S BY TID, S2 BY grTID;
describe JO;

-- drop unneded variables  

FJO = foreach JO generate S::TID, S::CPU, S::jobsInTask, S::AvgCPUtime;
STORE FJO INTO 'CPUtimesPerTask.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');