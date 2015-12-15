#!/bin/bash

echo "  *******************************  importing jobs table  *******************************"
echo " between $1 and $2 "


sqoop import --direct --connect "jdbc:oracle:thin:@//ADCR1-ADG-S.cern.ch:10121/ADCR.cern.ch" \
    --table ATLAS_PANDA.JOBSARCHIVED4 \
    --where "MODIFICATIONTIME between TO_DATE('$1','YYYY-MM-DD HH24:MI:SS') and TO_DATE('$2','YYYY-MM-DD HH24:MI:SS') " \
    --username ATLAS_PANDAMON_READER --password  \
    -m 1 --as-avrodatafile --target-dir /atlas/analytics/panda/jobs_TEST/$3 \
    --columns PANDAID,JOBDEFINITIONID,SCHEDULERID,PILOTID,CREATIONTIME,CREATIONHOST,MODIFICATIONTIME,MODIFICATIONHOST,ATLASRELEASE,TRANSFORMATION,HOMEPACKAGE,PRODSERIESLABEL,PRODSOURCELABEL,PRODUSERID,ASSIGNEDPRIORITY,CURRENTPRIORITY,ATTEMPTNR,MAXATTEMPT,JOBSTATUS,JOBNAME,MAXCPUCOUNT,MAXCPUUNIT,MAXDISKCOUNT,MAXDISKUNIT,IPCONNECTIVITY,MINRAMCOUNT,MINRAMUNIT,STARTTIME,ENDTIME,CPUCONSUMPTIONTIME,CPUCONSUMPTIONUNIT,COMMANDTOPILOT,TRANSEXITCODE,PILOTERRORCODE,PILOTERRORDIAG,EXEERRORCODE,EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG,COMPUTINGSITE,COMPUTINGELEMENT,PRODDBLOCK,DISPATCHDBLOCK,DESTINATIONDBLOCK,DESTINATIONSE,NEVENTS,GRID,CLOUD,CPUCONVERSION,SOURCESITE,DESTINATIONSITE,TRANSFERTYPE,TASKID,CMTCONFIG,STATECHANGETIME,PRODDBUPDATETIME,LOCKEDBY,RELOCATIONFLAG,JOBEXECUTIONID,VO,PILOTTIMING,WORKINGGROUP,PROCESSINGTYPE,PRODUSERNAME,NINPUTFILES,COUNTRYGROUP,BATCHID,PARENTID,SPECIALHANDLING,JOBSETID,CORECOUNT,NINPUTDATAFILES,INPUTFILETYPE,INPUTFILEPROJECT,INPUTFILEBYTES,NOUTPUTDATAFILES,OUTPUTFILEBYTES,JOBMETRICS,WORKQUEUE_ID,JEDITASKID,JOBSUBSTATUS,ACTUALCORECOUNT,REQID,MAXRSS,MAXVMEM,MAXPSS,AVGRSS,AVGVMEM,AVGSWAP,AVGPSS,MAXWALLTIME \
    --map-column-java PANDAID=Long,CREATIONTIME=String,STARTTIME=String,ENDTIME=String,MODIFICATIONTIME=String,JOBDEFINITIONID=Long,ASSIGNEDPRIORITY=Long,CURRENTPRIORITY=Long,ATTEMPTNR=Integer,MAXATTEMPT=Integer,MAXCPUCOUNT=Long,MAXDISKCOUNT=Long,MINRAMCOUNT=Long,CPUCONSUMPTIONTIME=Long,PILOTERRORCODE=Integer,EXEERRORCODE=Integer,SUPERRORCODE=Integer,DDMERRORCODE=Integer,BROKERAGEERRORCODE=Integer,JOBDISPATCHERERRORCODE=Integer,TASKBUFFERERRORCODE=Integer,NEVENTS=Long,TASKID=Long,STATECHANGETIME=String,PRODDBUPDATETIME=String,RELOCATIONFLAG=Integer,JOBEXECUTIONID=Long,NINPUTFILES=Integer,PARENTID=Long,JOBSETID=Long,CORECOUNT=Integer,NINPUTDATAFILES=Integer,INPUTFILEBYTES=Long,NOUTPUTDATAFILES=Integer,OUTPUTFILEBYTES=Long,WORKQUEUE_ID=Integer,JEDITASKID=Long,ACTUALCORECOUNT=Integer,REQID=Long,MAXRSS=Long,MAXVMEM=Long,MAXPSS=Long,AVGRSS=Long,AVGVMEM=Long,AVGSWAP=Long,AVGPSS=Long,MAXWALLTIME=Long 


echo "DONE"