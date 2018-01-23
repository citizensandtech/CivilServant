from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime
import app.controller
import os, argparse, sys
from utils.common import PageType

#documentation at
#https://github.com/ui/rq-scheduler

BASE_DIR = os.path.dirname(os.path.realpath(__file__))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("experiment",
                        help="The experiment")

    parser.add_argument("job",
                         choices=["intervene", "tidy", "archive_submissions"],
                         help="The job associated with the experiment")

    parser.add_argument("interval",
                        default = 120, # default 2 minutes
                        help="Interval between tasks in seconds (default 2 minutes)")
    parser.add_argument("-e", '--env',
                        choices=['development', 'test', 'production'],
                        required = False,
                        help="Run within a specific environment. Otherwise run under the environment defined in the environment variable CS_ENV")
    parser.add_argument("-p", "--profile",
                        required = False,
                        action = 'store_true',
                        help="Run the performance profiler and save the results in the logs/profiles directory")

    args = parser.parse_args()

    # if the user specified the environment, set it here
    if args.env!=None:
        os.environ['CS_ENV'] = args.env
    
    queue_name = os.environ['CS_ENV']
    scheduler = Scheduler(queue_name = os.environ['CS_ENV'], connection=Redis())


    timeout_seconds = 172800 ## two days in seconds
    if(timeout_seconds <= int(args.interval) + 3600):
        timeout_seconds = int(args.interval) + 3600
    ttl = int(args.interval) + 180

    experiment_file = os.path.join(BASE_DIR, "config", "experiments") + "/" + args.experiment + ".yml"
    if(os.path.isfile(experiment_file) == False):
        print("File {0} not found. Ignoring schedule command.".format(experiment_file))
        sys.exit(1)


    if(args.job == "intervene"):
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.conduct_sticky_comment_experiment,
                args=[args.experiment],
                kwargs={'_profile': args.profile},
                interval=int(args.interval),
                repeat=None,
                timeout = timeout_seconds
                result_ttl = ttl)
    elif(args.job == "tidy"):
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.remove_experiment_replies,
                args=[args.experiment],
                kwargs={'_profile': args.profile},
                interval=int(args.interval),
                repeat=None,
                timeout = timeout_seconds,
                result_ttl = ttl)
    elif(args.job == "archive_submissions"):
        scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=app.controller.archive_experiment_submission_metadata,
                args=[args.experiment],
                kwargs={'_profile': args.profile},
                interval=int(args.interval),
                repeat=None,
                timeout = timeout_seconds)

if __name__ == '__main__':
    main()
