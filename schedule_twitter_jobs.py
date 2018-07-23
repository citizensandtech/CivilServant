from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime
import app.controller
import os,argparse
from utils.common import PageType

#documentation at
#https://github.com/ui/rq-scheduler

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
                        required = True,
                        choices=["fetch_lumen_notices", "parse_lumen_notices_for_twitter_accounts", "fetch_twitter_users", "fetch_twitter_snapshot_and_tweets", "fetch_twitter_tweets"],
                        help="The controller function to call.")

    parser.add_argument("--lumen_delta_days",
                        required = False,
                        default= None,
                        help="For fetch_lumen_notices; fetch all notices received more than lumen_delta_days (in days) ago .")

    parser.add_argument("--snapshot_delta_min",
                        required = False,
                        default= None,
                        help="For fetch_twitter_snapshot_and_tweets; for all users older than snapshot_delta_min (in minutes), need to fetch new snapshots.")

    parser.add_argument("--statuses_backfill",
                        required = False,
                        default= None,
                        action='store_true',
                        help="For fetch_twitter_tweets; if backfill, finds tweetes for all twitter users, disregarding TwitterUser.CS_oldest_tweets_archived job state.")

    parser.add_argument("--interval",
                        default = 3600, # default 60 min = 60*60 = 3600 seconds
                        help="Interval (in seconds) between tasks in seconds (default 60 seconds)")

    parser.add_argument("-e", '--env',
                        choices=['development', 'test', 'production'],
                        required = False,
                        help="Run within a specific environment. Otherwise run under the environment defined in the environment variable CS_ENV")

    parser.add_argument("-q", '--queue',
                        choices=['development', 'test', 'production', 'production2'],
                        required = False,
                        help="Run within a specific queue. Otherwise run under the environment defined in the environment variable CS_ENV")

    args = parser.parse_args()

    # if the user specified the environment, set it here
    if args.env!=None:
        os.environ['CS_ENV'] = args.env

    if args.queue!=None:
        queue_name = args.queue
    else:
        queue_name = os.environ['CS_ENV']

    scheduler = Scheduler(queue_name = queue_name, connection=Redis())

    ttl = max(172800, int(args.interval) + 3600) # max of (2days in seconds, args.interval + 1 hr)
    timeout = max(60*60*24, int(args.interval) + 300) # max of (3hrs in seconds, args.interval + 50min)


    if args.function =="fetch_lumen_notices":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_lumen_notices,
                args=[args.lumen_delta_days],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl,
                timeout = timeout)
    elif args.function =="parse_lumen_notices_for_twitter_accounts":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.parse_lumen_notices_for_twitter_accounts,
                args=[],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl,
                timeout = timeout)
    elif args.function =="fetch_twitter_users":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_twitter_users,
                args=[],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl,
                timeout = timeout)
    elif args.function =="fetch_twitter_snapshot_and_tweets":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_twitter_snapshot_and_tweets,
                args=[args.snapshot_delta_min],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl,
                timeout = timeout)
    elif args.function =="fetch_twitter_tweets":                    
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_twitter_tweets,
                args=[args.statuses_backfill],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl,
                timeout = timeout)


if __name__ == '__main__':
    main()
