-- remove results directory
rmf results/Doug

REGISTER '/usr/lib/pig/piggybank.jar';
REGISTER '/usr/lib/pig/lib/avro-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;

-- load part of the data
PAN = LOAD '/atlas/analytics/panda/jobs/2015-08-*' USING AvroStorage();
DESCRIBE PAN;

-- filter out what we need
PA = filter PAN by PRODSOURCELABEL=='user' and NOT PRODUSERNAME=='gangarbt' and (INPUTFILEPROJECT=='data15_13TeV' OR INPUTFILEPROJECT=='mc15_13TeV:mc15_13TeV' OR INPUTFILEPROJECT=='mc15_13TeV' OR INPUTFILEPROJECT=='data15_13TeV:data15_13TeV')


GR = GROUP PA BY INPUTFILETYPE;
tGR = foreach GR generate group, COUNT(PA.INPUTFILETYPE);
dump tGR;


-- and (INPUTFILETYPE matches  'DAOD.*');

-- select only columns we are interested in
P = foreach PA generate COUNTRYGROUP, CLOUD, ASSIGNEDPRIORITY, CURRENTPRIORITY, FLATTEN(myfuncs.deriveDurationAndCPUeffNEW(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME)) as (wall_time:int, cpu_eff:float, queue_time:int);
DESCRIBE P;

-- check distribution of jobs over clouds
CL = group P by CLOUD;
tCL = foreach CL generate group, COUNT(P.CLOUD);
dump tCL;

-- split jobs according to cloud into US and nonUS sites
USsites = filter P by CLOUD matches '.*US.*';
NonUSsites = filter P by not CLOUD matches '.*US.*';

- group the jobs according to users provenance
gUSsites = group USsites by COUNTRYGROUP;
NusersOfUSsites = foreach gUSsites generate group, COUNT(USsites.COUNTRYGROUP);
WaitTimesOfUsersOfUSsites = foreach gUSsites generate group, AVG(USsites.queue_time);
store NusersOfUSsites into 'results/Doug/US_users_priorities/NusersOfUSsites';
store WaitTimesOfUsersOfUSsites into 'results/Doug/US_users_priorities/WaitTimesOfUsersOfUSsites';

-- group the jobs by both users provenance and cloud
gAll = group P by (COUNTRYGROUP, CLOUD);
WaitTimePerCloudOrigin = foreach gAll generate group, AVG(P.WAITTIME);
nJobsPerCloudOrigin = foreach gAll generate group, COUNT(P.WAITTIME);
store WaitTimePerCloudOrigin into 'results/Doug/US_users_priorities/WaitTimePerCloudOrigin';
store nJobsPerCloudOrigin into 'results/Doug/US_users_priorities/nJobsPerCloudOrigin';

