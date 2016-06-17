from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime
import app.controller
import os,argparse
from utils.common import PageType

#documentation at
#https://github.com/ui/rq-scheduler


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("sub",
                        help="The subreddit to query (or all for the frontpage)")

    parser.add_argument("pagetype",
                        choices=["new", "top", "contr", "hot"],
                        help="For front pages, what page to query")
    parser.add_argument("interval",
                        default = 120, # default 2 minutes
                        help="Interval between tasks in seconds (default 2 minutes)")
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

    page_type = getattr(PageType, args.pagetype.upper())

    ttl = 172800 ## two days in seconds
    if(ttl <= int(args.interval) + 3600):
        ttl = int(args.interval) + 3600

    if(args.sub =="all"):
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_reddit_front,
                args=[page_type],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl)
#                result_ttl=int(args.interval)+10)
                ## we set the result_ttl to longer than the interval
                ## so the job gets rescheduled
    else:
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.fetch_subreddit_front,
                args=[args.sub, page_type],
                interval=int(args.interval),
                repeat=None,
                result_ttl = ttl)
#                result_ttl=int(args.interval)+10)

if __name__ == '__main__':
    main()
