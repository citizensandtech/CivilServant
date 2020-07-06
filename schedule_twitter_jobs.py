import inspect
import random
import sys

from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime, timedelta
import app.controller
import os, argparse
import schedule_twitter_jobs
from utils.common import DbEngine
import json
import math

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
ENV = os.environ['CS_ENV']

### LOAD SQLALCHEMY SESSION
db_session = DbEngine(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV)).new_session()

# LOAD LOGGER
log = app.cs_logger.get_logger(ENV, BASE_DIR)

# documentation at
# https://github.com/ui/rq-scheduler

"""

when starting up these jobs, best to offset them all a bit (by a couple minutes)
fetch_lumen_notices: every 3 hours, look for posts that are from at most 2 days ago
parse_lumen_notices_for_twitter_accounts: every 1 hour
fetch_twitter_users: every 1 hour
fetch_twitter_snapshot_and_tweets: every 24 hours, get new snapshots for users who haven't had a snapshot in the last 23.3 hours
fetch_twitter_tweets: every 1 hour

python schedule_twitter_jobs.py --function fetch_lumen_notices                  --lumen_delta_days 2        --interval 10800 --env development
python schedule_twitter_jobs.py --function parse_lumen_notices_for_twitter_accounts                         --interval 3600 --env development
python schedule_twitter_jobs.py --function fetch_twitter_users                                              --interval 3600 --env development
python schedule_twitter_jobs.py --function fetch_twitter_snapshot_and_tweets    --snapshot_delta_min 1400   --interval 86400 --env development
python schedule_twitter_jobs.py --function fetch_twitter_tweets                                             --interval 3600 --env development

python schedule_twitter_jobs.py --function fetch_twitter_tweets                 --statuses_backfill          --interval 3600 --env development

"""


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--function",
                        required=True,
                        choices=["fetch_lumen_notices", "parse_lumen_notices_for_twitter_accounts",
                                 "fetch_twitter_users", "fetch_twitter_snapshot_and_tweets", "fetch_twitter_tweets",
                                 "report_calculations", "twitter_generate_random_id_users",
                                 "twitter_match_comparison_groups"
                                 ],
                        help="The controller function to call.")

    parser.add_argument("--lumen_delta_days",
                        required=False,
                        default=None,
                        help="For fetch_lumen_notices; fetch all notices received more than lumen_delta_days (in days) ago .")

    parser.add_argument("--snapshot_delta_min",
                        required=False,
                        default=None,
                        help="For fetch_twitter_snapshot_and_tweets; for all users older than snapshot_delta_min (in minutes), need to fetch new snapshots.")

    parser.add_argument("--statuses_backfill",
                        required=False,
                        default=None,
                        action='store_true',
                        help="For fetch_twitter_tweets; if backfill, finds tweetes for all twitter users, disregarding TwitterUser.CS_oldest_tweets_archived job state.")

    parser.add_argument("--interval",
                        default=3600,  # default 60 min = 60*60 = 3600 seconds
                        help="Interval (in seconds) between tasks in seconds (default 60 seconds)")

    parser.add_argument("-e", '--env',
                        # choices=['development', 'test', 'production'],
                        required=False,
                        help="Run within a specific environment. Otherwise run under the environment defined in the environment variable CS_ENV")

    parser.add_argument("-q", '--queue',
                        choices=['development', 'test', 'production', 'production2'],
                        required=False,
                        help="Run within a specific queue. Otherwise run under the environment defined in the environment variable CS_ENV")

    parser.add_argument("--n_tasks",
                        required=False,
                        default=1,
                        type=int,
                        help="Number of concurrent tasks. Currently only supported for fetch_twitter_tweets.")

    args = parser.parse_args()

    # if the user specified the environment, set it here
    if args.env != None:
        os.environ['CS_ENV'] = args.env

    if args.queue != None:
        queue_name = args.queue
    else:
        queue_name = os.environ['CS_ENV']

    scheduler = Scheduler(queue_name=queue_name, connection=Redis())

    SECONDS_IN_DAY = 60 * 60 * 24

    ttl = max(2 * SECONDS_IN_DAY, int(args.interval) + 3600)  # max of (2days in seconds, args.interval + 1 hr)
    timeout = max(SECONDS_IN_DAY, int(args.interval) + 300)  # max of (3hrs in seconds, args.interval + 50min)

    # LOAD Experiment details
    with open(os.path.join(BASE_DIR, 'config', '{env}.json'.format(env=os.environ['CS_ENV']))) as f:
        config = json.load(f)
    try:
        experiment_onboarding_days = int(config["experiment_onboarding_days"])
        experiment_collection_days = int(config["experiment_collection_days"])
        experiment_start_date = datetime.strptime(config["experiment_start_date"], '%Y-%m-%d')
        user_rand_frac = config["user_rand_frac"]
        random_users_daily_limit = config["random_users_daily_limit"]
        random_users_target_additions = config["random_users_target_additions"]
        fetch_tweets_schedule_random_offset = config["fetch_tweets_schedule_random_offset"]
        today = datetime.utcnow()
        log.info('Loaded experiment start date: {}. Today is :{}'.format(experiment_start_date, today))
        time_til_experiment = experiment_start_date - today
        log.info('Time until experiment is: {}'.format(time_til_experiment))
        if time_til_experiment.days >= 1:
            sleep_secs = time_til_experiment.seconds
            log.info('Experiment start date more than 1 day in the future. Sleeping for {}'.format(sleep_secs))
        if time_til_experiment.days <= -1:
            log.info('Experiment exists in past! This might be because you are restarting it. However it could also '
                     'be because you forgot to change the experiment start date, that would be a bad thing!')
        # Experiment has two stages.
        #  1) Onboarding, while we are still adding new users
        #  2) Collection. Collect happens during onboarding too, but continues afterwards to collect data on onboarded users
        days_already_done = -1 * time_til_experiment.days
        # plus padding to always round up to nearest day
        onboarding_days_left = experiment_onboarding_days - days_already_done + 1
        collection_days_left = experiment_collection_days
        if (onboarding_days_left <= 0) or (collection_days_left <= 0):
            raise ValueError('Experiment ended in the past')
        onboarding_seconds = SECONDS_IN_DAY * onboarding_days_left
        collection_seconds = SECONDS_IN_DAY * collection_days_left
        total_experiment_seconds = onboarding_seconds + collection_seconds
        onboarding_repeats = math.ceil(onboarding_seconds / int(args.interval))
        total_experiment_repeats = math.ceil(total_experiment_seconds / int(args.interval))
        # if you pass None to repeats it will continue indefinitely which is what we want for the undefined behaviour
        log.info('Loaded experiment with experiment_onboarding_days: {}, onboarding seconds: {}'.format(
            experiment_onboarding_days, onboarding_seconds))
        log.info('Loaded experiment with experiment_collection_days: {}, collection seconds: {}'.format(
            experiment_collection_days, collection_seconds))

    except KeyError:  # this means that part of the config is unspecified
        onboarding_repeats = None
        total_experiment_repeats = None
        collection_seconds = None
    except ValueError as e:
        log.error(e)
        sys.exit(1)

    if args.function == "fetch_lumen_notices":
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=app.controller.fetch_lumen_notices,
            args=[args.lumen_delta_days],
            interval=int(args.interval),
            repeat=onboarding_repeats,
            result_ttl=ttl,
            timeout=timeout)
    elif args.function == "parse_lumen_notices_for_twitter_accounts":
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=app.controller.parse_lumen_notices_for_twitter_accounts,
            args=[],
            interval=int(args.interval),
            repeat=onboarding_repeats,
            result_ttl=ttl,
            timeout=timeout)
    elif args.function == "fetch_twitter_users":
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=app.controller.fetch_twitter_users,
            args=[],
            interval=int(args.interval),
            repeat=onboarding_repeats,
            result_ttl=ttl,
            timeout=timeout)
    elif args.function == "fetch_twitter_snapshot_and_tweets":
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=app.controller.fetch_twitter_snapshot_and_tweets,
            args=[args.snapshot_delta_min],
            interval=int(args.interval),
            repeat=total_experiment_repeats,
            result_ttl=ttl,
            timeout=timeout)
    elif args.function == "fetch_twitter_tweets":
        repeats = onboarding_repeats if args.statuses_backfill else total_experiment_repeats
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=schedule_twitter_jobs.schedule_fetch_tweets,
            args=(args, ttl, timeout, queue_name, repeats, collection_seconds, user_rand_frac,
                  fetch_tweets_schedule_random_offset),
            interval=int(args.interval),
            repeat=repeats,
            result_ttl=ttl,
            timeout=timeout)
    elif args.function == "twitter_generate_random_id_users":
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=app.controller.fetch_twitter_random_id_users,
            args=[random_users_daily_limit, random_users_target_additions],
            interval=int(args.interval),
            repeat=None,
            result_ttl=ttl,
            timeout=timeout)
    elif args.function == "twitter_match_comparison_groups":
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=app.controller.twitter_match_comparison_groups,
            args=[],
            interval=int(args.interval),
            repeat=None,
            result_ttl=ttl,
            timeout=timeout)
    else:
        raise NotImplementedError("Unimplimented function: {}".format(args.function))


def schedule_fetch_tweets(args, ttl, timeout, queue_name, repeats, collection_seconds, user_rand_frac, random_offset):
    fill_start_time = datetime.utcnow()
    scheduler_concurrent = Scheduler(queue_name=queue_name + '_concurrent', connection=Redis())
    log.info('FILLTASKS: n_tasks is {}'.format(args.n_tasks))
    for task in range(args.n_tasks):
        random_offset_seconds = random.randint(1, random_offset)
        scheduler_concurrent.schedule(
            scheduled_time=datetime.utcnow() + timedelta(seconds=random_offset_seconds),
            func=app.controller.fetch_twitter_tweets,
            kwargs={"backfill": args.statuses_backfill,
                    "fill_start_time": fill_start_time,
                    "collection_seconds": collection_seconds,
                    "user_rand_frac": user_rand_frac},
            interval=int(args.interval),
            repeat=0,
            result_ttl=ttl,
            timeout=timeout)


if __name__ == '__main__':
    main()
