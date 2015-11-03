-- remove results directory 
rmf results/Panda/US_users_priorities

-- this code tries to aswer the following question:
-- do US ATLAS users have any advantage in getting their code to run
-- thanks to "over the pledge" resources available at US ATLAS sites

-- registering libraries we will need
REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';

REGISTER 'myudfs.py' using jython as myfuncs;

-- load data from Oct 2015
PAN = LOAD '/atlas/analytics/panda/jobs/2015-10-*' USING AvroStorage();

-- filter out what we don't need
PA = filter PAN by PRODSOURCELABEL=='user' and NOT PRODUSERNAME=='gangarbt';

P = foreach PA generate COUNTRYGROUP, CLOUD, ASSIGNEDPRIORITY, CURRENTPRIORITY, FLATTEN(myfuncs.deriveDurationAndCPUeffNEW(CREATIONTIME,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME)) as (wall_time:int, cpu_eff:float, queue_time:int);
DESCRIBE P;


-- check distribution of jobs over clouds
CL = group P by CLOUD;
tCL = foreach CL generate group, COUNT(P.CLOUD);
dump tCL;

-- split jobs according to cloud into US and nonUS sites
USsites = filter P by CLOUD matches '.*US.*';
NonUSsites = filter P by not CLOUD matches '.*US.*';

-- group the jobs according to users provenance
gUSsites = group USsites by COUNTRYGROUP;
NusersOfUSsites = foreach gUSsites generate group, COUNT(USsites.COUNTRYGROUP);
WaitTimesOfUsersOfUSsites = foreach gUSsites generate group, AVG(USsites.queue_time);
store NusersOfUSsites into 'results/Panda/US_users_priorities/NusersOfUSsites';
store WaitTimesOfUsersOfUSsites into 'results/Panda/US_users_priorities/WaitTimesOfUsersOfUSsites';

gNonUSsites = group NonUSsites by COUNTRYGROUP;
NusersOfNonUSsites = foreach gNonUSsites generate group, COUNT(NonUSsites.COUNTRYGROUP);
WaitTimesOfUsersOfNonUSsites = foreach gNonUSsites generate group, AVG(NonUSsites.queue_time);  
store NusersOfNonUSsites into 'results/Panda/US_users_priorities/NusersOfNonUSsites';
store WaitTimesOfUsersOfNonUSsites into 'results/Panda/US_users_priorities/WaitTimesOfUsersOfNonUSsites';

-- group the jobs by both users provenance and cloud
gAll = group P by (COUNTRYGROUP, CLOUD);
WaitTimePerCloudOrigin = foreach gAll generate group, AVG(P.queue_time);
nJobsPerCloudOrigin = foreach gAll generate group, COUNT(P.queue_time);
store WaitTimePerCloudOrigin into 'results/Panda/US_users_priorities/WaitTimePerCloudOrigin';
store nJobsPerCloudOrigin into 'results/Panda/US_users_priorities/nJobsPerCloudOrigin';
