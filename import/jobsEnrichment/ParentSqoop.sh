#!/bin/bash

echo "  *******************************  sqooping jobs table  *******************************"
echo " between $1 and $2 "

sqoop import --direct --connect "jdbc:oracle:thin:@//ADCR1-ADG-S.cern.ch:10121/ADCR.cern.ch" \
 --username ATLAS_PANDAMON_READER --password  XXXX \
 --query "select OLDPANDAID, NEWPANDAID, RELATIONTYPE from ATLAS_PANDA.JEDI_JOB_RETRY_HISTORY WHERE INS_UTC_TSTAMP between TO_DATE('$1 00:00:00','YYYY-MM-DD HH24:MI:SS') and TO_DATE('$2 00:00:00','YYYY-MM-DD HH24:MI:SS') AND \$CONDITIONS " \
 -m 1  --target-dir /atlas/analytics/job_parents/$1     

echo "DONE"
# --as-avrodatafile  removed so it stores in csv
 
