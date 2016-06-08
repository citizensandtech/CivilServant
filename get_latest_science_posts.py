import reddit.connection
import simplejson as json
import copy
import reddit.praw_utils as praw_utils
import reddit.queries

def get_posts():
  r = reddit.connection.connect()
  print(json.dumps(reddit.queries.get_new_as_dict(r, "science")))
  
if __name__ == "__main__":
  get_posts()