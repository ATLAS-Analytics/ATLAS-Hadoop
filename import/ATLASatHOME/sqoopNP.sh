#!/bin/bash

echo "  *******************************  importing ATLAS@HOME table  *******************************"
#echo " between $1 and $2 "
sqoop import \
    --connect "jdbc:mysql://boincdb.cern.ch:5506/atlas_production" \
    --username boincadm \
    --password XXXXXXXX \
    --query 'select r.id as result_id, r.outcome, r.create_time as result_create_time, r.cpu_time, r.elapsed_time, r.granted_credit as job_credit, r.app_version_id, r.appid, r.sent_time, r.received_time, r.mod_time, r.opaque as job_ncore, h.domain_name as host_name, h.p_fpops/1e9 as host_flops, h.p_ncpus as host_ncpus, h.avg_turnaround as host_avg_tt, h.error_rate as host_err_rate FROM result r INNER JOIN host h ON r.hostid = h.id WHERE $CONDITIONS' \
    --where 'outcome!=0 and appid=2' \
    --as-avrodatafile \
    --target-dir /atlas/analytics/ATLASatHOME/test \
    -m 1

