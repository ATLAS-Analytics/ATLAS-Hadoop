#!/bin/bash

# this script is called once a day
# finds newly transfered file and uses it to update parent child values in ES.

echo "  *******************************  start  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobsEnrichment/

FILES=/tmp/*.update
for f in $FILES
do
  echo "Processing $f file..."
  if python3.5 updater.py $f ; then
    echo "processed correctly. removing file"
    rm $f
  else
    echo "processing $f failed."
done

echo "Updates DONE."
