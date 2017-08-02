#!/bin/bash

echo "  *******************************  importing ATLAS@HOME table  *******************************"
#echo " between $1 and $2 "
sqoop import \
    --connect "jdbc:mysql://boincdb.cern.ch:5506/atlas" \
    --username boincadm \
    --password XXXXXXXX \
    --query 'select r.id as result_id, r.create_time as result_create_time, r.cpu_time, r.elapsed_time, r.granted_credit, r.app_version_id, r.appid, r.sent_time, r.received_time, r.mod_time, r.opaque, h.* FROM result r INNER JOIN host h ON r.hostid = h.id WHERE $CONDITIONS' \
    --where 'outcome=1 and cpu_time>100 and appid=2' \
    --as-avrodatafile \
    --target-dir /atlas/analytics/ATLASatHOME/test \
    -m 1

