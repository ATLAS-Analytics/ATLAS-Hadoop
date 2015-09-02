#!/bin/bash

# this script is called from a cron once a day after script importing the new data
# it runs a pig script to index the new data in ES.

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/
#DateToProcess=$(date +%Y-%m-%d)
DateToProcess=$(date +%Y-%m-%d -d "-4day")
echo "Indexing... "${DateToProcess}
pig -4 log4j.properties -f toES.pig -param INPD=${DateToProcess}
pig -4 log4j.properties -f toEScl.pig -param INPD=${DateToProcess}
pig -4 log4j.properties -f toESuc.pig -param INPD=${DateToProcess}




