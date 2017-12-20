#!/bin/bash

echo "  *******************************  importing ATLAS@HOME table  *******************************"
#echo " between $1 and $2 "
sqoop import \
    --connect "jdbc:mysql://dbod-sixtrack.cern.ch:5513/sixt_production" \
    --username XXXXX \
    --password XXXXX \
    --query 'select r.id as result_id, r.outcome, r.create_time as result_create_time, r.cpu_time, r.elapsed_time, r.granted_credit as job_credit, r.app_version_id as result_app_version_id, r.appid as result_app_id, r.sent_time as result_sent_time, r.received_time as result_received_time, r.mod_time as result_mod_time, r.opaque as job_ncore, r.name as result_name, h.domain_name as host_name, h.p_fpops/1e9 as host_flops, h.p_ncpus as host_ncpus, h.avg_turnaround as host_avg_tt, h.error_rate as host_err_rate, w.* FROM result r INNER JOIN host h ON r.hostid = h.id INNER JOIN workunit w ON r.workunitid=w.id WHERE $CONDITIONS' \
    --where 'outcome!=0' \
    --as-avrodatafile \
    --target-dir /atlas/analytics/ATLASatHOME/production \
    -m 1

