#!/usr/bin/env bash

start_standard(){
### Fetch lumen notices every 3 hours
# TODO make this variable with n-tasks

echo "launch workers"
rqscheduler &
rqworker $CS_ENV &
rqworker $CS_ENV"_concurrent" &
rqworker $CS_ENV"_concurrent" &

echo "Fetch lumen notices every 3 hours"
python schedule_twitter_jobs.py --function fetch_lumen_notices --lumen_delta_days 2 --interval 10800

## Parse lumen notices for twitter accounts: every 3 hours
echo "Parse lumen notices for twitter accounts: every 3 hours"
python schedule_twitter_jobs.py --function parse_lumen_notices_for_twitter_accounts --interval 10800

## Fetch Twitter Users: every 3 hours
## Fetch Twitter Users: every 3 hours
python schedule_twitter_jobs.py --function fetch_twitter_users --interval 10800

# Backfill Twitter tweets: Every 12 hours
echo "Backfill Twitter tweets: Every 12 hours"
python schedule_twitter_jobs.py --function fetch_twitter_tweets --statuses_backfill --interval 43200

# Fetch Twitter Tweets: Every twenty-four hours (once a day)
echo "Fetch Twitter Tweets: Every twenty-four hours (once a day)"
python schedule_twitter_jobs.py --function fetch_twitter_tweets --interval 86400 --n_tasks 2

## Fetch Twitter Account Snapshots & Tweet Counts: every 24 hours, get new snapshots for users who haven't had a snapshot in the last 23.3 hours
echo "Fetch Twitter Account Snapshots & Tweet Counts: every 24 hours, get new snapshots for users who haven't had a snapshot in the last 23.3 hours"
python schedule_twitter_jobs.py --function fetch_twitter_snapshot_and_tweets --snapshot_delta_min 1400 --interval 86400

python manage_scheduled_jobs.py show all

}

stop_all(){
python manage_scheduled_jobs.py purge all
rq empty -a
killall rqworker
killall rqscheduler
}

if [ -z $CS_ENV ]
    then
    echo "CS_ENV not defined, quitting."
    exit 1
fi

echo "Running with CS_ENV=$CS_ENV"

if [ $1 = "start" ]
    then
    echo "starting"
    start_standard
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
    start_standard
fi
