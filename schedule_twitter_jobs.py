import inspect
import sys

from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime
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
                                 "report_calculations",
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
                        help="Number of concurrent tasks. Currently only supports fetch_twitter_tweets.")

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

    ttl = max(2*SECONDS_IN_DAY, int(args.interval) + 3600)  # max of (2days in seconds, args.interval + 1 hr)
    timeout = max(SECONDS_IN_DAY, int(args.interval) + 300)  # max of (3hrs in seconds, args.interval + 50min)

    # LOAD Experiment details
    with open(os.path.join(BASE_DIR, 'config', '{env}.json'.format(env=os.environ['CS_ENV']))) as f:
        config = json.load(f)
    try:
        experiment_onboarding_days = config["experiment_onboarding_days"]
        experiment_collection_days = config["experiment_collection_days"]
        log.info('Loaded experiment with experiment_onboarding_days: {}'.format(experiment_onboarding_days))
        log.info('Loaded experiment with experiment_collection_days: {}'.format(experiment_collection_days))
    except KeyError:  # this means that the config is unspecified
        experiment_onboarding_days = None
        experiment_collection_days = None
    # Experiment has two stages.
    #  1) Onboarding, while we are still adding new users
    #  2) Collection. Collect happens during onboarding too, but continues afterwards to collect data on onboarded users
    if experiment_onboarding_days is not None and experiment_collection_days is not None:
        onboarding_seconds = SECONDS_IN_DAY * experiment_onboarding_days
        collection_seconds = SECONDS_IN_DAY * experiment_collection_days
        total_experiment_seconds = onboarding_seconds + collection_seconds
        onboarding_repeats = math.ceil(onboarding_seconds / int(args.interval))
        total_experiment_repeats = math.ceil(total_experiment_seconds / int(args.interval))
    else:
        # if you pass None to repeats it will continue indefinitely which is what we want for the undefined behaviour
        onboarding_repeats = None
        total_experiment_repeats = None

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
        repeats = onboarding_repeats if args.statuses_backfil else total_experiment_repeats
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=schedule_twitter_jobs.schedule_fetch_tweets,
            args=(args, ttl, timeout, queue_name, repeats, collection_seconds),
            interval=int(args.interval),
            repeat=repeats,
            result_ttl=ttl,
            timeout=timeout)
    elif args.function == "report_calculations":
        calc_str = str(('onboarding_repeats',onboarding_repeats,
                        "total_experiment_repeats",total_experiment_repeats))
        sys.stdout.write(calc_str)


def schedule_fetch_tweets(args, ttl, timeout, queue_name, repeats, collection_seconds):
    fill_start_time = datetime.utcnow()
    scheduler_concurrent = Scheduler(queue_name=queue_name+'_concurrent', connection=Redis())
    for task in range(args.n_tasks):
        scheduler_concurrent.schedule(
            scheduled_time=datetime.utcnow(),
            func=app.controller.fetch_twitter_tweets,
            args=[args.statuses_backfill, fill_start_time, collection_seconds],
            interval=int(args.interval),
            repeat=repeats,
            result_ttl=ttl,
            timeout=timeout)



if __name__ == '__main__':
    main()
