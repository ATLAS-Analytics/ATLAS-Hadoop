#!/bin/bash

hdfs dfs -rm -skipTrash /atlas/analytics/ATLASatHOME/production/*
hdfs dfs -rmdir  /atlas/analytics/ATLASatHOME/production

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/ATLASatHOME
./sqoopProdViewWP.sh

pig -4 log4j.properties -f prodToESuc.pig
