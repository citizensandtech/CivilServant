#!/usr/bin/env bash

date=$(date +%Y%m%d)
time=$(date +%H%M)
filename="$date".txt

cd /usr/local/civilservant

mkdir -p platform/logs/mysql_size/
size=$(du -chBM data/mysql | grep total | cut -f1)

echo $date $time $size >> platform/logs/mysql_size/$filename

