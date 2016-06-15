from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime
import app.controller
import os

#documentation at
#https://github.com/ui/rq-scheduler

scheduler = Scheduler(queue_name=os.environ['CS_ENV'], connection=Redis())
print("SCHEDULE FOR {0}".format(os.environ['CS_ENV']))
print
print(scheduler.get_jobs())
print
#import pdb; pdb.set_trace()
#for job in scheduler.get_jobs():
#  scheduler.cancel(job.id)

