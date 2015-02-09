#!/bin/bash

# this script is called from a cron once a day after script importing the new data

# it runs a pig script to extract overflow info
cd /afs/cern.ch/user/i/ivukotic/ATLAS-data-analytics/pigCodes/Panda/OverflowMatrix/
pig OverflowMatrix.pig

# then it runs a python code that takes results and uploads it to FSB (GAE)
python OverflowMatrixSender.py



