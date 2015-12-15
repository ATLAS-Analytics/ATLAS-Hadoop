#!/bin/bash

echo "  *******************************  importing jobs parameters table  *******************************  "

#sqoop job --exec JOBSPARAMETERSARCHIVEDimport 
#hdfs dfs -mkdir /atlas/analytics/panda/JOBSPARAMETERSARCHIVED/jobparameters.${DateToMoveTo}
#hdfs dfs -mv   '/atlas/analytics/panda/JOBSPARAMETERSARCHIVED/part*' /atlas/analytics/panda/JOBSPARAMETERSARCHIVED/jobparameters.${DateToMoveTo}/.

# saved job was created using this command:
#sqoop job -Doracle.sessionTimeZone="+01:00" -Doracle.userTimeZone="+01:00" -D sqoop.metastore.client.record.password=true --create JOBSPARAMETERSARCHIVEDimport  -- import --connect "jdbc:oracle:thin:@//db-d0002.cern.ch:10654/int8r.cern.ch" --table ATLAS_PANDAARCH.JOBPARAMSTABLE_ARCH --username DMUSER --P --split-by PANDAID --fields-terminated-by , --escaped-by \\ --enclosed-by '\"' --target-dir /atlas/analytics/panda/JOBSPARAMETERSARCHIVED --columns PANDAID,MODIFICATIONTIME,JOBPARAMETERS --check-column PANDAID --incremental append --last-value 2352434197 2273521915
echo "DONE"

