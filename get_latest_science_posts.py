import reddit.connection
import simplejson as json
import copy
import reddit.praw_utils as praw_utils
import reddit.queries

"""
DEPRECATED - use queue_jobs.py instead
"""

def get_posts():
  conn = reddit.connection.Connect()
  r = conn.connect(controller="GetTopScience")
  print(json.dumps(reddit.queries.get_new_as_dict(r, "science")))
  
if __name__ == "__main__":
  get_posts()