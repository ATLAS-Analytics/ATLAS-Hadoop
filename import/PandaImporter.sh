#!/bin/bash

# this script is called from a cron once a day
# it scoops panda job archive info

echo "  *******************************  importing jobs table  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import

startDate=$(date +%Y-%m-%d -d "-4day")
endDate=$(date +%Y-%m-%d  -d "-3day")
./JobImportwp.sh ${startDate} ${endDate}

echo "DONE"


#echo "  *******************************  importing jobs parameters table  *******************************  "

#sqoop job --exec JOBSPARAMETERSARCHIVEDimport 
#hdfs dfs -mkdir /atlas/analytics/panda/JOBSPARAMETERSARCHIVED/jobparameters.${DateToMoveTo}
#hdfs dfs -mv   '/atlas/analytics/panda/JOBSPARAMETERSARCHIVED/part*' /atlas/analytics/panda/JOBSPARAMETERSARCHIVED/jobparameters.${DateToMoveTo}/.

# saved job was created using this command:
#sqoop job -Doracle.sessionTimeZone="+01:00" -Doracle.userTimeZone="+01:00" -D sqoop.metastore.client.record.password=true --create JOBSPARAMETERSARCHIVEDimport  -- import --connect "jdbc:oracle:thin:@//db-d0002.cern.ch:10654/int8r.cern.ch" --table ATLAS_PANDAARCH.JOBPARAMSTABLE_ARCH --username DMUSER --P --split-by PANDAID --fields-terminated-by , --escaped-by \\ --enclosed-by '\"' --target-dir /atlas/analytics/panda/JOBSPARAMETERSARCHIVED --columns PANDAID,MODIFICATIONTIME,JOBPARAMETERS --check-column PANDAID --incremental append --last-value 2352434197 2273521915

#echo "DONE"

#echo "  *******************************  importing jobs status table  *******************************  "
#sqoop job --exec JOBSSTATUSimport

#hdfs dfs -mkdir /atlas/analytics/panda/JOBS_STATUSLOG/${DateToMoveTo:0:7}/${DateToMoveTo}
#hdfs dfs -mv '/atlas/analytics/panda/JOBS_STATUSLOG/part*' /atlas/analytics/panda/JOBS_STATUSLOG/${DateToMoveTo:0:7}/${DateToMoveTo}

#sqoop job -D sqoop.metastore.client.record.password=true --create JOBSSTATUSimport  -- import --connect "jdbc:oracle:thin:@//db-d0002.cern.ch:10654/int8r.cern.ch" --table ATLAS_PANDA.JOBS_STATUSLOG --username DMUSER --P --split-by PANDAID --as-avrodatafile --target-dir /atlas/analytics/panda/JOBS_STATUSLOG --columns PANDAID,MODIFICATIONTIME,JOBSTATUS,PRODSOURCELABEL,CLOUD,COMPUTINGSITE --check-column MODIFICATIONTIME --incremental append --last-value '01-Jan-10'

#echo "DONE"

# echo "  *******************************  importing panda log table  *******************************  "
# sdate=$(date --date="2 days ago" +%d-%b-%Y)
# edate=$(date --date="1 days ago" +%d-%b-%Y)
# sqoop import --connect "jdbc:oracle:thin:@//adcr1-adg-s.cern.ch:10121/adcr.cern.ch" --table ATLAS_PANDA.PANDALOG --username ATLAS_PANDABIGMON --P --as-avrodatafile --target-dir /atlas/analytics/panda/PandaLog --columns NAME,MODULE,LOGUSER,TYPE,PID,LOGLEVEL,LEVELNAME,TIME,FILENAME,LINE,MESSAGE,BINTIME --where "BINTIME>'${sdate}' and BINTIME<='${edate}'" -m 1 --append
#
#
# hdfs dfs -mkdir /atlas/analytics/panda/PandaLog/${DateToMoveTo:0:7}/${DateToMoveTo}
# hdfs dfs -mv '/atlas/analytics/panda/PandaLog/part*' /atlas/analytics/panda/PandaLog/${DateToMoveTo:0:7}/${DateToMoveTo}

echo "ALL DONE"