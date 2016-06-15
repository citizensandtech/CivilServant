from redis import Redis
from rq_scheduler import Scheduler
from datetime import datetime
import app.controller
import os

#documentation at
#https://github.com/ui/rq-scheduler

scheduler = Scheduler(queue_name = os.environ['CS_ENV'], connection=Redis())

scheduler.schedule(
    scheduled_time=datetime.utcnow(),
    func=app.controller.fetch_reddit_front,
    interval=60,
    repeat=None,
    result_ttl=0)

#scheduler.schedule(
#    scheduled_time=datetime.utcnow(),
#    func=app.controller.fetch_reddit_front,
#    interval=60,
#    repeat=10)
