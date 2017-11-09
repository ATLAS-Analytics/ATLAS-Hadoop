#!/bin/bash

hdfs dfs -rm /atlas/analytics/ATLASatHOME/production/*
hdfs dfs -rmdir /atlas/analytics/ATLASatHOME/production

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/ATLASatHOME
./sqoopProdWP.sh

pig -4 log4j.properties -f prodToESuc.pig
