#!/bin/bash

# this script is called from a cron every day and indexes data from previous day

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

echo "  *******************************  indexing FTS data  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/FTS/

startDate=$(date -u '+%Y-%m-%d' -d "-12hour")
#ind=$(date -u '+%Y-%m-%d' -d "-2hour")
echo "start date: ${startDate}"  
#echo "index : ${ind}"  
python preloading.py

pig -4 log4j.properties -f fts_to_ES_uc.pig -param ININD=${startDate}
echo "DONE."
