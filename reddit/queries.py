import praw
import reddit.connection
import reddit.praw_utils as praw_utils
import copy


def get_new_as_dict(r, subname):
  
  posts = []
  sub = r.subreddit(subname)
  for post in sub.new(limit=100):
    p = praw_utils.prepare_post_for_json(copy.copy(post))    
    posts.append(p)

  return posts
