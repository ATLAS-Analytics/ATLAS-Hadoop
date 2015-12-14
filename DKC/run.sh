#!/bin/bash

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/DKC
echo "Indexing... CL "${DateToProcess}
pig -4 log4j.properties -f toEScl.pig 




