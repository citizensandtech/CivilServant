from redis import Redis
from rq.registry import StartedJobRegistry

redis_conn = Redis()
registry = StartedJobRegistry('production', connection=redis_conn)

queue_jobs = registry.get_queue().job_ids

print("{} jobs in queue with {} unique ids".format(
  len(queue_jobs), len(set(queue_jobs))))
