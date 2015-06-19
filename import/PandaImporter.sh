#!/bin/bash

# this script is called from a cron once a day
# it scoops panda job archive info

echo "importing jobs table"

sqoop job --exec JOBSARCHIVEDimport 

# moves the produced files into directory with the date in it's name
DateToMoveTo=$(date +%Y-%m-%d)
hdfs dfs -mkdir /atlas/analytics/panda/JOBSARCHIVED/jobs.${DateToMoveTo:0:7}/jobs.${DateToMoveTo}
hdfs dfs -mv '/atlas/analytics/panda/JOBSARCHIVED/part*' /atlas/analytics/panda/JOBSARCHIVED/jobs.${DateToMoveTo:0:7}/jobs.${DateToMoveTo}

/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/run.sh 

# saved job was created using this command:
#sqoop job -Doracle.sessionTimeZone="+01:00" -Doracle.userTimeZone="+01:00" -D sqoop.metastore.client.record.password=true --create JOBSARCHIVEDimport  -- import --connect "jdbc:oracle:thin:@//db-d0002.cern.ch:10654/int8r.cern.ch" --table ATLAS_PANDAARCH.JOBSARCHIVED --username DMUSER --P --split-by PANDAID --as-avrodatafile --target-dir /atlas/analytics/panda/JOBSARCHIVED --columns PANDAID,JOBDEFINITIONID,SCHEDULERID,PILOTID,CREATIONTIME,CREATIONHOST,MODIFICATIONTIME,MODIFICATIONHOST,ATLASRELEASE,TRANSFORMATION,HOMEPACKAGE,PRODSERIESLABEL,PRODSOURCELABEL,PRODUSERID,ASSIGNEDPRIORITY,CURRENTPRIORITY,ATTEMPTNR,MAXATTEMPT,JOBSTATUS,JOBNAME,MAXCPUCOUNT,MAXCPUUNIT,MAXDISKCOUNT,MAXDISKUNIT,IPCONNECTIVITY,MINRAMCOUNT,MINRAMUNIT,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME,CPUCONSUMPTIONUNIT,COMMANDTOPILOT,TRANSEXITCODE,PILOTERRORCODE,PILOTERRORDIAG,EXEERRORCODE,EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG,COMPUTINGSITE,COMPUTINGELEMENT,PRODDBLOCK,DISPATCHDBLOCK,DESTINATIONDBLOCK,DESTINATIONSE,NEVENTS,GRID,CLOUD,CPUCONVERSION,SOURCESITE,DESTINATIONSITE,TRANSFERTYPE,TASKID,CMTCONFIG,STATECHANGETIME,PRODDBUPDATETIME,LOCKEDBY,RELOCATIONFLAG,JOBEXECUTIONID,VO,PILOTTIMING,WORKINGGROUP,PROCESSINGTYPE,PRODUSERNAME,NINPUTFILES,COUNTRYGROUP,BATCHID,PARENTID,SPECIALHANDLING,JOBSETID,CORECOUNT,NINPUTDATAFILES,INPUTFILETYPE,INPUTFILEPROJECT,INPUTFILEBYTES,NOUTPUTDATAFILES,OUTPUTFILEBYTES,JOBMETRICS,WORKQUEUE_ID,JEDITASKID,JOBSUBSTATUS,ACTUALCORECOUNT --check-column PANDAID --incremental append --last-value 2352434197

echo "importing jobs parameters table"

sqoop job --exec JOBSPARAMETERSARCHIVEDimport 
hdfs dfs -mkdir /atlas/analytics/panda/JOBSPARAMETERSARCHIVED/jobparameters.${DateToMoveTo}
hdfs dfs -mv   '/atlas/analytics/panda/JOBSPARAMETERSARCHIVED/part*' /atlas/analytics/panda/JOBSPARAMETERSARCHIVED/jobparameters.${DateToMoveTo}/.

# saved job was created using this command:
#sqoop job -Doracle.sessionTimeZone="+01:00" -Doracle.userTimeZone="+01:00" -D sqoop.metastore.client.record.password=true --create JOBSPARAMETERSARCHIVEDimport  -- import --connect "jdbc:oracle:thin:@//db-d0002.cern.ch:10654/int8r.cern.ch" --table ATLAS_PANDAARCH.JOBPARAMSTABLE_ARCH --username DMUSER --password usr4dmine --split-by PANDAID --fields-terminated-by , --escaped-by \\ --enclosed-by '\"' --target-dir /atlas/analytics/panda/JOBSPARAMETERSARCHIVED --columns PANDAID,MODIFICATIONTIME,JOBPARAMETERS --check-column PANDAID --incremental append --last-value 2352434197 2273521915

echo "importing jobs status table"
sqoop job --exec JOBSSTATUSimport

#sqoop job -Doracle.sessionTimeZone="+01:00" -Doracle.userTimeZone="+01:00" -D sqoop.metastore.client.record.password=true --create JOBSSTATUSimport  -- import --connect "jdbc:oracle:thin:@//db-d0002.cern.ch:10654/int8r.cern.ch" --table ATLAS_PANDA.JOBS_STATUSLOG --username DMUSER --P --split-by PANDAID --as-avrodatafile --target-dir /atlas/analytics/panda/JOBS_STATUSLOG --columns PANDAID,MODIFICATIONTIME,JOBSTATUS,PRODSOURCELABEL,CLOUD,COMPUTINGSITE --check-column MODIFICATIONTIME --incremental append --last-value '01-Jan-10'

echo "importing panda log table"
sdate=$(date --date="2 days ago" +%d-%b-%Y)
edate=$(date --date="1 days ago" +%d-%b-%Y)
sqoop import --connect "jdbc:oracle:thin:@//adcr1-adg-s.cern.ch:10121/adcr.cern.ch" --table ATLAS_PANDA.PANDALOG --username ATLAS_PANDABIGMON --P --as-avrodatafile --target-dir /atlas/analytics/panda/PandaLog --columns NAME,MODULE,LOGUSER,TYPE,PID,LOGLEVEL,LEVELNAME,TIME,FILENAME,LINE,MESSAGE,BINTIME --where "BINTIME>'${sdate}' and BINTIME<='${edate}'" -m 1 --append