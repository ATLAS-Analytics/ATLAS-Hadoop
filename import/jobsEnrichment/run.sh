#!/bin/bash

# this script is called once a day
# it scoops jobs parent info and stores it in hdfs
# runs a Spark job summing info up per pandaID and taskID writes back to hdfs
# it takes jobs data from ES, adds parent info, stores in a new index.

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

echo "  *******************************  importing parent/child table  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobsEnrichment/

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-48hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-24hour")

echo "start date: ${startDate}"  
echo "end date: ${endDate}"  
#echo "file name: ${fileName}"  
#echo "index : ${ind}"  
./ParentSqoopWP.sh "${startDate}" "${endDate}"
echo "Sqooping DONE."

#pyspark indexer.py

echo "copy file to UC. Will actually index it directly there. Check how expesive is modifying docs."
hdfs dfs -getmerge /atlas/analytics/job_parents/${startDate} /tmp/${startDate}
scp /tmp/${startDate} uct2-collectd.mwt2.org:/tmp/.

#hdfs dfs -get "/atlas/analytics/panda/jobs/${fileName}" /tmp/ivukotic/.

#pig -4 log4j.properties -f toEScl.pig -param INPD=${fileName} -param ININD=${ind}
#echo "Indexing CL DONE."
