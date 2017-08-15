#!/bin/bash

# this script is called from a cron once a day

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobsStates

startDate=$(date +%Y-%m-%d -d "-4day")
endDate=$(date +%Y-%m-%d  -d "-3day")


./JobStatesSqoopWP_ADCR_ADG.sh ${startDate} ${endDate}

echo "DONE"
