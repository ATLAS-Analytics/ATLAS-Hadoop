#!/bin/bash

# this script is called from a cron every hour
# it scoops panda job archive info and then indexes it in ES at clemson.

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

echo "  *******************************  importing jobs table  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobs/

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")
fileName=$(date -u '+%Y-%m-%d_%H' -d "-2hour")
ind=$(date -u '+%Y-%m-%d' -d "-2hour")
echo "start date: ${startDate}"  
echo "end date: ${endDate}"  
echo "file name: ${fileName}"  
echo "index : ${ind}"  
./JobSqoopWP.sh "${startDate}" "${endDate}" "${fileName}"
echo "Sqooping DONE."

pig -4 log4j.properties -f toEScl.pig -param INPD=${fileName} -param ININD=${ind}
echo "Indexing DONE."
