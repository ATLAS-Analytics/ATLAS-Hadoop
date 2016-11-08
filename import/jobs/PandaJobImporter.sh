#!/bin/bash

# this script is called from a cron every hour
# it scoops panda job archive info and then indexes it in ES at clemson.

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

echo "  *******************************  importing jobs table  *******************************"

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/jobs/

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")
fileName=$(date -u '+%Y-%m-%d_%H' -d "-2hour")
ind=$(date -u '+%Y-%m-%d' -d "-2hour")
echo "start date: ${startDate}"  
echo "end date: ${endDate}"  
echo "file name: ${fileName}"  
echo "index : ${ind}"  
./JobSqoopWP.sh "${startDate}" "${endDate}" "${fileName}"
echo "Sqooping DONE."


#hdfs dfs -get "/atlas/analytics/panda/jobs/${fileName}" /tmp/ivukotic/.

#IN=''
#for f in "/tmp/ivukotic/${fileName}/p*"
#do
#    IN=(${IN} $f)
#done
#echo $IN
#java -jar ~/avro-tools-1.8.0.jar concat $IN /tmp/ivukotic/$fileName.avro

echo "not uploading to Google"
#/afs/cern.ch/user/i/ivukotic/google-cloud-sdk/bin/gsutil cp /tmp/ivukotic/${fileName}.avro gs://panda_job_data/${fileName}.avro
#rm -rf /tmp/ivukotic/${fileName}*
#echo "Uploaded to Google Storage"

#/afs/cern.ch/user/i/ivukotic/google-cloud-sdk/bin/bq  load --source_format AVRO panda_dataset.jobs  gs://panda_job_data/${fileName}.avro
#echo "Loaded into Google BigQuery"



pig -4 log4j.properties -f toEScl.pig -param INPD=${fileName} -param ININD=${ind}
echo "Indexing CL DONE."

#pig -4 log4j.properties -f toEScern.pig -param INPD=${fileName} -param ININD=${ind}
#echo "Indexing CERN DONE."
echo "not indexing at CERN."
