#!/bin/bash

# this script is called from a cron once a day after script importing the new data
# it runs a pig script to extract overflow info

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/OverflowMatrix/
DateToProcess=$(date +%Y-%m-%d)
echo "Processing... "${DateToProcess}
pig -f OverflowMatrix.pig -param INPD=${DateToProcess} 

# then it runs a python code that takes results and uploads it to FSB (GAE)
echo "Processing in python and sending data to FSB"
python OverflowMatrixSender.py



