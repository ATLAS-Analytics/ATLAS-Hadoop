#!/bin/bash

# this script is called from a cron every hour
# it scoops panda job archive info and then indexes it in ES at clemson.

echo "  *******************************  importing jobs table  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobs/

startDate=$(date -u '+%Y-%m-%d %T' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %T' -d "-1hour")
fileName=$(date -u '+%Y-%m-%d_%H' -d "-2hour")
./JobSqoopWP.sh ${startDate} ${endDate} ${fileName}
echo "Sqooping DONE."

fileName=$(date -u '+%Y-%m-%d' -d "-2hour")
pig -4 log4j.properties -f toES.pig -param INPD=${fileName} ININD=${ind}

echo "Indexing DONE."
