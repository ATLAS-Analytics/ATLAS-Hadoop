-- remove results directory
rmf results/Doug

REGISTER '/usr/lib/pig/piggybank.jar';
REGISTER '/usr/lib/pig/lib/avro-*.jar';

-- load part of the data
PAN = LOAD '/atlas/analytics/panda/JOBSARCHIVED/jobs.2015-08-0*' USING AvroStorage();
DESCRIBE PAN;

-- filter out what we don't need
PA = filter PAN by COMPUTINGSITE=='ANALY_AGLT2_SL6' or COMPUTINGSITE=='ANALY_MWT2_SL6' or COMPUTINGSITE=='ANALY_BNL_SHORT' or COMPUTINGSITE=='ANALY_BNL_LONG' or COMPUTINGSITE=='ANALY_SFU' or COMPUTINGSITE=='ANALY_SLAC';

-- select only columns we are interested in
P = foreach PA generate TRANSFERTYPE,(long)MODIFICATIONTIME, JOBSTATUS,  STARTTIME, ENDTIME, COMPUTINGSITE, DESTINATIONSE, (int)ATTEMPTNR, REPLACE(SOURCESITE,'ANALY_','') as SOURCE, DESTINATIONSITE, (int)PILOTERRORCODE, (long)INPUTFILEBYTES, (long)CPUCONSUMPTIONTIME,  ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME;

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
WaitTimesOfUsersOfUSsites = foreach gUSsites generate group, AVG(USsites.WAITTIME);
store NusersOfUSsites into 'results/Panda/US_users_priorities/NusersOfUSsites';
store WaitTimesOfUsersOfUSsites into 'results/Panda/US_users_priorities/WaitTimesOfUsersOfUSsites';

-- group the jobs by both users provenance and cloud
gAll = group P by (COUNTRYGROUP, CLOUD);
WaitTimePerCloudOrigin = foreach gAll generate group, AVG(P.WAITTIME);
nJobsPerCloudOrigin = foreach gAll generate group, COUNT(P.WAITTIME);
store WaitTimePerCloudOrigin into 'results/Panda/US_users_priorities/WaitTimePerCloudOrigin';
store nJobsPerCloudOrigin into 'results/Panda/US_users_priorities/nJobsPerCloudOrigin';

