#!/bin/bash

echo "  *******************************  importing ATLAS@HOME table  *******************************"
#echo " between $1 and $2 "
sqoop import \
    --connect "jdbc:mysql://dbod-sixtrack.cern.ch:5513/sixt_production" \
    --username XXXXX \
    --password XXXXX \
    --query 'select * FROM analyticsTable WHERE $CONDITIONS' \
    --where 'outcome!=0' \
    --as-avrodatafile \
    --target-dir /atlas/analytics/ATLASatHOME/production \
    -m 1

