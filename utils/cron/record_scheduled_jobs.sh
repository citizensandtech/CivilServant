#!/usr/bin/env bash

date=$(date +%Y%m%d)
time=$(date +%H%M)
filename="$date"_"$time".txt

cd /usr/local/civilservant
source python3env/bin/activate

cd platform
source config/environment_variables.sh

mkdir -p logs/jobs/$date
python3 manage_scheduled_jobs.py show all 2>&1 > logs/jobs/$date/$filename

