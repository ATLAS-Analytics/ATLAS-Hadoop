#!/bin/bash

# this script is called once a day
# it scoops jobs parent info and stores it in hdfs
# runs a Spark job summing info up per pandaID and taskID writes back to hdfs
# it takes jobs data from ES, adds parent info, stores in a new index.

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

echo "  *******************************  importing parent/child table  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobsEnrichment/

startDate=$(date -u '+%Y-%m-%d' -d "-48hour")
endDate=$(date -u '+%Y-%m-%d' -d "-24hour")

echo "start date: ${startDate}"  
echo "end date: ${endDate}"  
#echo "file name: ${fileName}"  
#echo "index : ${ind}"  
./ParentSqoopWP.sh "${startDate}" "${endDate}"
echo "Sqooping DONE."

#pyspark indexer.py

echo "copy file to UC. Will index it from there."
hdfs dfs -getmerge /atlas/analytics/job_parents/${startDate} /tmp/${startDate}.update
scp /tmp/${startDate}.update uct2-collectd.mwt2.org:/tmp/.
rm /tmp/${startDate}.update

#echo "import DONE."
