#!/bin/bash

# this script is called from a cron once a day
# it scoops panda job archive info

echo "  *******************************  importing jobs table  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobsStates

startDate=$(date +%Y-%m-%d -d "-4day")
endDate=$(date +%Y-%m-%d  -d "-3day")

echo "  *******************************  importing jobs status table  *******************************  "

./JobStatesSqoopWP.sh ${startDate} ${endDate}

echo "DONE"