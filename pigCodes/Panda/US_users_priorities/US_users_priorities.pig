-- remove results directory 
rmf results/Panda/US_users_priorities
-- this code tries to aswer the following question:
-- do US ATLAS users have any advantage in getting their code to run
-- thanks to "over the pledge" resources available at US ATLAS sites

-- redo Slimming
-- rmf PandaSlimmed;
-- register '/usr/lib/pig/piggybank.jar' ;
-- DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- P = LOAD 'Panda' USING CSVLoader  as (PANDAID:long, MODIFICATIONTIME:long, JOBDEFINITIONID:long, SCHEDULERID:chararray, PILOTID:chararray, CREATIONTIME:long, CREATIONHOST:chararray, MODIFICATIONHOST:chararray, ATLASRELEASE:chararray, TRANSFORMATION:chararray, HOMEPACKAGE:chararray, PRODSERIESLABEL:chararray, PRODSOURCELABEL:chararray, PRODUSERID:chararray, ASSIGNEDPRIORITY:long, CURRENTPRIORITY:long, ATTEMPTNR:int, MAXATTEMPT:int, JOBSTATUS:chararray, JOBNAME:chararray, MAXCPUCOUNT:long, MAXCPUUNIT:chararray, MAXDISKCOUNT:long, MAXDISKUNIT:chararray, IPCONNECTIVITY:chararray, MINRAMCOUNT:long, MINRAMUNIT:chararray, STARTTIME:long, ENDTIME:long, CPUCONSUMPTIONTIME:long, CPUCONSUMPTIONUNIT:chararray, COMMANDTOPILOT:chararray, TRANSEXITCODE:chararray, PILOTERRORCODE::int, PILOTERRORDIAG:chararray, EXEERRORCODE:int, EXEERRORDIAG:chararray, SUPERRORCODE:int, SUPERRORDIAG:chararray, DDMERRORCODE:int, DDMERRORDIAG:chararray, BROKERAGEERRORCODE:int, BROKERAGEERRORDIAG:chararray, JOBDISPATCHERERRORCODE:int, JOBDISPATCHERERRORDIAG:chararray, TASKBUFFERERRORCODE:int, TASKBUFFERERRORDIAG:chararray, COMPUTINGSITE:chararray, COMPUTINGELEMENT:chararray, PRODDBLOCK:chararray, DISPATCHDBLOCK:chararray, DESTINATIONDBLOCK:chararray, DESTINATIONSE:chararray, NEVENTS:long, GRID:chararray, CLOUD:chararray, CPUCONVERSION:float, SOURCESITE:chararray, DESTINATIONSITE:chararray, TRANSFERTYPE:chararray, TASKID:long, CMTCONFIG:chararray, STATECHANGETIME:long, PRODDBUPDATETIME:long, LOCKEDBY:chararray, RELOCATIONFLAG:int, JOBEXECUTIONID:long, VO:chararray, PILOTTIMING:chararray, WORKINGGROUP:chararray, PROCESSINGTYPE:chararray, PRODUSERNAME:chararray, NINPUTFILES:int, COUNTRYGROUP:chararray, BATCHID:chararray, PARENTID:long, SPECIALHANDLING:chararray, CORECOUNT:int, NINPUTDATAFILES:int, INPUTFILETYPE:chararray, INPUTFILEPROJECT:chararray, INPUTFILEBYTES:long, NOUTPUTDATAFILES:int,OUTPUTFILEBYTES:long);

-- B = foreach P generate PANDAID, CREATIONTIME, ASSIGNEDPRIORITY, CURRENTPRIORITY, JOBSTATUS, STARTTIME, ENDTIME, CPUCONSUMPTIONTIME, COMPUTINGSITE, DESTINATIONSE, NEVENTS, CLOUD, SOURCESITE, DESTINATIONSITE, TRANSFERTYPE, PILOTTIMING, NINPUTFILES, COUNTRYGROUP, NINPUTDATAFILES, INPUTFILEBYTES, ENDTIME - STARTTIME as WALLTIME, STARTTIME - CREATIONTIME as WAITTIME;

-- store B into 'PandaSlimmed';


P = LOAD 'PandaSlimmed' as (PANDAID, CREATIONTIME, ASSIGNEDPRIORITY, CURRENTPRIORITY, JOBSTATUS, STARTTIME, ENDTIME, CPUCONSUMPTIONTIME, COMPUTINGSITE, DESTINATIONSE, NEVENTS, CLOUD, SOURCESITE, DESTINATIONSITE, TRANSFERTYPE, PILOTTIMING, NINPUTFILES, COUNTRYGROUP, NINPUTDATAFILES, INPUTFILEBYTES, WALLTIME:long, WAITTIME:long);

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
WaitTimesOfUsersOfUSsites = foreach gUSsites generate group, AVG(USsites.WAITTIME);
store NusersOfUSsites into 'results/Panda/US_users_priorities/NusersOfUSsites';
store WaitTimesOfUsersOfUSsites into 'results/Panda/US_users_priorities/WaitTimesOfUsersOfUSsites';

gNonUSsites = group NonUSsites by COUNTRYGROUP;
NusersOfNonUSsites = foreach gNonUSsites generate group, COUNT(NonUSsites.COUNTRYGROUP);
WaitTimesOfUsersOfNonUSsites = foreach gNonUSsites generate group, AVG(NonUSsites.WAITTIME);  
store NusersOfNonUSsites into 'results/Panda/US_users_priorities/NusersOfNonUSsites';
store WaitTimesOfUsersOfNonUSsites into 'results/Panda/US_users_priorities/WaitTimesOfUsersOfNonUSsites';

-- group the jobs by both users provenance and cloud
gAll = group P by (COUNTRYGROUP, CLOUD);
WaitTimePerCloudOrigin = foreach gAll generate group, AVG(P.WAITTIME);
nJobsPerCloudOrigin = foreach gAll generate group, COUNT(P.WAITTIME);
store WaitTimePerCloudOrigin into 'results/Panda/US_users_priorities/WaitTimePerCloudOrigin';
store nJobsPerCloudOrigin into 'results/Panda/US_users_priorities/nJobsPerCloudOrigin';
