#!/usr/bin/env bash

start_standard(){
### Fetch lumen notices every 3 hours
echo "starting with $1 threads"
echo "launch workers"

#put the rq scheduler in the background
rqscheduler &

# the unadorned-queuename queue has just one worker
rqworker $CS_ENV &

for i in $(seq $1 $END)
    do
    echo "Launching concurrent worker $i "
#    important to use `rqworker` and not `rq worker` because the stop command kills `rqworker`.
    rqworker $CS_ENV"_concurrent" &
    done

logfile="logs/CivilServant_"$CS_ENV".log"
echo "logfile is "$logfile

echo "Fetch lumen notices every 3 hours"
python schedule_twitter_jobs.py --function fetch_lumen_notices --lumen_delta_days 2 --interval 10800 2> $logfile

## Parse lumen notices for twitter accounts: every 3 hours
echo "Parse lumen notices for twitter accounts: every 3 hours"
python schedule_twitter_jobs.py --function parse_lumen_notices_for_twitter_accounts --interval 10800 2> $logfile

## Fetch Twitter Users: every 3 hours
echo Fetch Twitter Users: every 3 hours
python schedule_twitter_jobs.py --function fetch_twitter_users --interval 10800 2> $logfile

# Backfill Twitter tweets: Every 12 hours
echo "Backfill Twitter tweets: Every 12 hours"
python schedule_twitter_jobs.py --function fetch_twitter_tweets --statuses_backfill --interval 43200 --n_tasks $1 2> $logfile

# Fetch Twitter Tweets: Every twenty-four hours (once a day)
echo "Fetch Twitter Tweets: Every twenty-four hours (once a day)"
python schedule_twitter_jobs.py --function fetch_twitter_tweets --interval 86400 --n_tasks $1 2> $logfile

## Fetch Twitter Account Snapshots & Tweet Counts: every 24 hours, get new snapshots for users who haven't had a snapshot in the last 23.3 hours
echo "Fetch Twitter Account Snapshots & Tweet Counts: every 24 hours, get new snapshots for users who haven't had a snapshot in the last 23.3 hours"
python schedule_twitter_jobs.py --function fetch_twitter_snapshot_and_tweets --snapshot_delta_min 1400 --interval 86400 2> $logfile

python manage_scheduled_jobs.py show all 2> $logfile

}

stop_all(){
python manage_scheduled_jobs.py purge all
rq empty -a
killall rqworker
killall rqscheduler
}

if [ -z $CS_ENV ]
    then
    echo "trying to source environment variables."
    source config/environment_variables.sh
    if [ -z $CS_ENV ]
        then
        echo "couldn't get CS_ENV out of the script, exting"
        exit 1
        else
        echo "Found CS_ENV from script"
    fi
fi

echo "Running with CS_ENV=$CS_ENV"

# the second argument represents the number of threads to use, if unset, default to 4.
if [ -z $2 ]
    then
    n_tasks=4
    else
    n_tasks=$2
fi

# the first argument is a comman, either to start, stop, or restart(stop and then start).
if [ $1 = "start" ]
    then
    echo "starting"
    start_standard $n_tasks
fi

if [ $1 = "stop" ]
    then
    echo "stopping"
    stop_all
fi

if [ $1 = "restart" ]
    then
    echo "restarting"
    stop_all
    start_standard $n_tasks
fi
