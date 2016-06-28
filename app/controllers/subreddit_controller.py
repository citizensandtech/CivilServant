import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from utils.common import PageType
from app.models import Base, SubredditPage, Subreddit, Post, User

class SubredditPageController:
  def __init__(self, subname, db_session, r, log):
    self.subname = subname
    self.db_session = db_session
    self.log = log
    self.r = r    


  def fetch_subreddit_page(self, pg_type, limit=100):
      posts = []
      fetched = []
      sub = self.r.get_subreddit(self.subname)

      # fetch subreddit posts from reddit
      try:
          if pg_type==PageType.TOP:
              fetched = sub.get_top(limit=limit)
          elif pg_type==PageType.CONTR:
              fetched = sub.get_controversial(limit=limit)
          elif pg_type==PageType.NEW:
              fetched = sub.get_new(limit=limit)   
          elif pg_type==PageType.HOT:
              fetched = sub.get_hot(limit=limit)   
      except:
          self.log.error("Error querying /r/{0} {1} page".format(self.subname, pg_type.name), extra=sys.exc_info()[0] )
          print(sys.exc.info()[0])
          return []         
      self.log.info("Queried /r/{0} {1} page".format(self.subname, pg_type.name))

      # add sub to subreddit table if not already there
      try:
          if self.archive_subreddit(sub):
            self.log.info("Saved new record for subreddit /r/{0}".format(self.subname))
      except:
          self.log.error("Failed to save new record for subreddit /r/{0}".format(self.subname), extra=sys.exc_info()[0] )

      # save subreddit posts to database

      self.log.info(json.dumps(fetched))
      try:
          for post in fetched:
              new_post = post.json_dict if("json_dict" in dir(post)) else post['data'] ### TO HANDLE TEST FIXTURES
              posts.append(new_post)
              is_new_post = self.archive_post(new_post)
              is_new_user = self.archive_user(new_post['author'], datetime.datetime.fromtimestamp(new_post['created']))
          self.log.info("Saved posts from /r/{0} {1} page.".format(self.subname, pg_type.name))
      except:
          self.log.error("Error Saving posts from /r/{0} {1} page".format(self.subname, pg_type.name), extra=sys.exc_info()[0] )
              
      return posts


  def archive_subreddit_page(self, pg_type=PageType.HOT):
      posts = self.fetch_subreddit_page(pg_type)
      subreddit_page = SubredditPage(created_at = datetime.datetime.now(),
                             page_type = pg_type.value, 
                             page_data = json.dumps(posts))
      self.db_session.add(subreddit_page)
      self.db_session.commit()


  """ 
      returns True if it archives a new subreddit. 
      returns False if the subreddit does not need to be archived.
  """
  def archive_subreddit(self, sub):
      queried_sub = self.db_session.query(Subreddit).filter(Subreddit.id == sub.id).first()

      # if sub not in table, add it
      if not queried_sub:
          new_sub = Subreddit(id = sub.id, 
                              name = sub.display_name)
          self.db_session.add(new_sub)
          self.db_session.commit()
          return True

      # else don't add it to subreddit table
      return False


  """ 
      note that 'post' is of type dictionary (has already been initially processed in fetch_subreddit_page)

      returns True if it archives a new post. 
      returns False if the post does not need to be archived.
  """
  def archive_post(self, post_info):
      queried_post = self.db_session.query(Post).filter(Post.id == post_info['id']).first()

      # if sub not in table, add it
      if not queried_post:
          new_post = Post(
                  id = post_info['id'],
                  subreddit_id = post_info['subreddit_id'].strip("t5_"), # janky
                  created = datetime.datetime.fromtimestamp(post_info['created_utc']),        
                  post_data = json.dumps(post_info))
          self.db_session.add(new_post)
          self.db_session.commit()
          return True

      # else don't add it to post table
      return False


  """ 
      seen_at is of type timestamp
      (to save on api calls, do not query reddit for user info!)

      returns True if it archives a new redditor. 
      returns False if the redditor was already in table.
  """
  def archive_user(self, username, seen_at):
      user = self.db_session.query(User).filter(User.name == username).first()

      if not user:
          new_user = User(
                  name = username,
                  id = None,
                  created = None,
                  first_seen = seen_at,
                  last_seen = seen_at, 
                  user_data = None)
          self.db_session.add(new_user)
          self.db_session.commit()
          return True
      else:
          if seen_at > user.last_seen:
              user.last_seen = seen_at
              self.db_session.commit()
          return False