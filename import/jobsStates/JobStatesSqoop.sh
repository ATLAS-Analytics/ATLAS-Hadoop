#!/bin/bash

echo "  *******************************  importing job status table  *******************************"
echo " between $1 and $2 "

sqoop import --direct --connect "jdbc:oracle:thin:@//db-d0002.cern.ch:10654/int8r.cern.ch" --table ATLAS_PANDA.JOBS_STATUSLOG --username DMUSER -P --split-by PANDAID --as-avrodatafile --target-dir /atlas/analytics/panda/jobs_status/$1 --columns PANDAID,MODIFICATIONTIME,JOBSTATUS,PRODSOURCELABEL,CLOUD,COMPUTINGSITE --where "MODIFICATIONTIME between TO_DATE('$1','YYYY-MM-DD') and TO_DATE('$2','YYYY-MM-DD') " --map-column-java PANDAID=Long,MODIFICATIONTIME=String

echo "DONE"

