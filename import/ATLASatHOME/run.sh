#!/bin/bash

hdfs dfs -rm -skipTrash /atlas/analytics/ATLASatHOME/test/*
hdfs dfs -rmdir /atlas/analytics/ATLASatHOME/test

cd /afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/ATLASatHOME
./sqoopWP.sh

pig -4 log4j.properties -f toESuc.pig
