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
parse_lumen_notices_for_twitter_accounts: every 2 hours
fetch_twitter_users: every 2 hours
fetch_twitter_snapshot_and_tweets: every 24 hours, get new snapshots for users who haven't had a snapshot in the last 23.3 hours
fetch_twitter_tweets: every 2 hours

python twitter_schedule_jobs.py --function fetch_lumen_notices                  --lumen_delta_days 2        --interval 180 --env development
python twitter_schedule_jobs.py --function parse_lumen_notices_for_twitter_accounts                         --interval 120 --env development
python twitter_schedule_jobs.py --function fetch_twitter_users                                              --interval 120 --env development
python twitter_schedule_jobs.py --function fetch_twitter_snapshot_and_tweets    --snapshot_delta_min 1400   --interval 1440 --env development
python twitter_schedule_jobs.py --function fetch_twitter_tweets                                             --interval 120 --env development


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

    parser.add_argument("--interval",
                        default = 60, # default 60 min
                        help="Interval (in minutes) between tasks in seconds (default 60 minutes)")

    parser.add_argument("-e", '--env',
                        choices=['development', 'test', 'production'],
                        required = False,
                        help="Run within a specific environment. Otherwise run under the environment defined in the environment variable CS_ENV")

    args = parser.parse_args()

    # if the user specified the environment, set it here
    if args.env!=None:
        os.environ['CS_ENV'] = args.env
    
    queue_name = os.environ['CS_ENV']
    scheduler = Scheduler(queue_name = os.environ['CS_ENV'], connection=Redis())

    ttl = 172800 ## two days in seconds
    if(ttl <= int(args.interval) + 3600):
        ttl = int(args.interval) + 3600 # args.interval + 1 hr

    if args.function =="fetch_lumen_notices":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_lumen_notices,
                args=[args.lumen_date],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl)
    elif args.function =="parse_lumen_notices_for_twitter_accounts":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.parse_lumen_notices_for_twitter_accounts,
                args=[],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl)
    elif args.function =="fetch_twitter_users":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_twitter_users,
                args=[],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl)
    elif args.function =="fetch_twitter_snapshot_and_tweets":
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_twitter_snapshot_and_tweets,
                args=[args.snapshot_delta_min],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl)
    elif args.function =="fetch_twitter_tweets":                    
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_twitter_tweets,
                args=[args.lumen_date],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl)


if __name__ == '__main__':
    main()
