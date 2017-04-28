import app.connections.reddit_connect
import simplejson as json
import copy
import app.connections.praw_utils as praw_utils
import app.connections..queries

"""
DEPRECATED - use queue_jobs.py instead
"""

def get_posts():
  conn = app.connections.reddit_connect
  r = conn.connect(controller="GetTopScience")
  print(json.dumps(reddit.queries.get_new_as_dict(r, "science")))
  
if __name__ == "__main__":
  get_posts()