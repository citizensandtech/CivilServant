import reddit.connection
import simplejson as json
import copy

r = reddit.connection.connect()

posts = []
sub = r.get_subreddit("science")
for post in sub.get_new(limit=100):
  p = copy.copy(post.__dict__)

  p['author'] = copy.copy(p['author'].__dict__)
  p['author']['reddit_session'] = None

  p['subreddit'] = copy.copy(p['subreddit'].__dict__)
  p['subreddit']['reddit_session']=None
  if "approved_by" in p.keys() and p['approved_by']:    
    p['approved_by'] = copy.copy(p['approved_by'].__dict__)
    p['approved_by']['reddit_session']=None

  p['reddit_session'] = None
  
  posts.append(p)

print(json.dumps(posts))