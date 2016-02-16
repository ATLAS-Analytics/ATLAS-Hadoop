#!/bin/bash

# this script is called from a cron once a day - from an uct2-int machine
# it creates a backup of the .kibana index of three ES clusters.

currDate=$(date +%Y-%m-%d)

curl -XPUT cl-analytics.mwt2.org:9200/_snapshot/my_backup/${currDate} 


