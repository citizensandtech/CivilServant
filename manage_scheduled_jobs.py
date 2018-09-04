import os,argparse
from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime, timedelta
import app.controller
import os

#documentation at
#https://github.com/ui/rq-scheduler

def main():
    
    parser = argparse.ArgumentParser()

    parser.add_argument("action",
                        choices = ['show', 'remove', 'purge'],
                        help = "Which action to run.")
    
    parser.add_argument("object",
                        help = "Which object to remove (if removing). Add 'all' where this argument is unused.")
    
    parser.add_argument("-e", '--env',
                      choices=['development', 'test', 'production'],
                      help="Run within a specific environment. Otherwise run under the environment defined in the environment variable CS_ENV")
    
    args = parser.parse_args()
    if args.env!=None:
        os.environ['CS_ENV'] = args.env

    queue_name = os.environ['CS_ENV']
    scheduler = Scheduler(queue_name=os.environ['CS_ENV'], connection=Redis())

    if(args.action == "show"):
        print("\n")
        print("=================================")
        print("  Job Schedule For {0}".format(os.environ['CS_ENV']))
        print("=================================")
        print("\n")
        for job in scheduler.get_jobs(until=timedelta(hours=24), with_times=True):
            print("ID: {1}\n    Job: {0}\n    Time: {2}\tInterval: {3}\n".format(job[0].description, job[0].id, job[1], job[0].meta["interval"]))
    elif(args.action == "remove"):
        if(args.object is None):
            print("Please specify the job to remove")
        else:
            jobs = scheduler.get_jobs()
            for job in jobs:
                if(args.object == job.id):
                    scheduler.cancel(job.id)
                    print("Job {0} cancelled from {1}".format(args.object, os.environ['CS_ENV']))
    elif(args.action == "purge"):
        count = 0
        for job in scheduler.get_jobs():
            count += 1
            scheduler.cancel(job.id)
        print("Purged {0} jobs from {1}".format(count, os.environ['CS_ENV']))

if __name__ == '__main__':
    main()
