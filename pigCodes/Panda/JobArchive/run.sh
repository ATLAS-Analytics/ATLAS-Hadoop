#!/bin/bash

# this script is called from a cron once a day after script importing the new data
# it runs a pig script to index the new data in ES.

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/
DateToProcess=$(date +%Y-%m-%d)
echo "Indexing... "${DateToProcess}
pig -f toES.pig -param INPD=${DateToProcess} -param IND=${DateToProcess:0:7}




