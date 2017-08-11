#!/bin/bash

echo "  *******************************  sqooping jobs table  *******************************"
echo " between $1 and $2 "

sqoop import --direct --connect "jdbc:oracle:thin:@//ADCR1-ADG-S.cern.ch:10121/ADCR.cern.ch" \
 --table ATLAS_PANDA.JEDI_JOB_RETRY_HISTORY     --username ATLAS_PANDAMON_READER --password  XXXX \
 -m 1 --as-avrodatafile --target-dir /atlas/analytics/job_parents/$1     \
 --where "INS_UTC_TSTAMP between TO_DATE('$1','YYYY-MM-DD HH24:MI:SS') and TO_DATE('$2','YYYY-MM-DD HH24:MI:SS') "


echo "DONE"
