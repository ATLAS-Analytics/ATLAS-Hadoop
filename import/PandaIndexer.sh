#!/bin/bash

# this script is called from a cron once a day - from an analytics machine
# it indexes the panda archived jobs from pig into ES
# has to be called few hours after PandaImporter. 


cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import

startDate=$(date +%Y-%m-%d -d "-4day")
endDate=$(date +%Y-%m-%d  -d "-3day")


echo "  ******************************* indexing Panda Jobs into ES **************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/
pig -4 log4j.properties -f toESuc.pig -param INPD=${startDate}


echo "DONE"
