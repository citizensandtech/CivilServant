#!/usr/bin/env bash

start_standard(){
echo "starting with $1 threads"
echo "launch workers"

#put the rq scheduler in the background
rqscheduler &

# the unadorned-queuename queue has just one worker
rqworker $CS_ENV &

# the tweet filling needs mutliprocessing
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


echo "Generate random ID users every half and ten minutes"
python schedule_twitter_jobs.py --function twitter_generate_random_id_users --interval  600 2> $logfile

echo "Match ID groups twice every three hours, like notice onboarding"
python schedule_twitter_jobs.py --function twitter_match_comparison_groups --interval 10800 2> $logfile

python manage_scheduled_jobs.py show all 2> $logfile

}

stop_all(){
python manage_scheduled_jobs.py purge all
rq empty $CS_ENV
rq empty $CS_ENV"_concurrent"
killall rqworker
killall rqscheduler
}

if [ -z ${1} ];
    then
    echo "no verb specified, exiting..."
    exit
    else
    echo "verb is $1"
fi

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

# the first argument is a command, either to start, stop, or restart(stop and then start).
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
