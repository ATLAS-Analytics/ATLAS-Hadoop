#!/bin/bash

# this script is called from a cron once a day - from an uct2-int machine
# it deletes .marvel-es-YYYY.mm.dd index older than 10 days.
# it deletes ddm-YYYY-mm-dd index older than 10 days.
 

DDM_date=$(date +%Y-%m-%d -d "-10days")
marvel_Date=$(date +%Y.%m.%d -d "-10days")

curl -XDELETE http://cl-analytics.mwt2.org:9200/ddm-${DDM_date} 
curl -XDELETE http://cl-analytics.mwt2.org:9200/.marvel-es-${marvel_Date} 

echo 'Done.'

