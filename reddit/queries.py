import praw
import reddit.connection
import reddit.praw_utils as praw_utils
import copy


def get_new_as_dict(r, subname,limit):
  posts = []
  sub = r.subreddit(subname)
  for post in sub.new(limit=limit):
    p = praw_utils.prepare_post_for_json(copy.copy(post))
    posts.append(p)
  return posts