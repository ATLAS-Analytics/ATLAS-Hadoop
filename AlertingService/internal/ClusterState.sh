#!/bin/bash
echo '--------------------'
date
a=$(curl cl-analytics.mwt2.org:9200/_cluster/health)
y=$(echo $a | grep 'yellow')
r=$(echo $a | grep 'red')
g=$(echo $a | grep 'green')
[[ !  -z  $y  ]] && echo 'cluster in yellow' | mail ivukotic@uchicago.edu
[[ !  -z  $r  ]] && echo 'cluster in red' | mail ivukotic@uchicago.edu
[[ !  -z  $g  ]] && echo 'cluster in green'
