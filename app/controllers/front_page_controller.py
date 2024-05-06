import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from utils.common import PageType, thing2dict
from app.models import Base, FrontPage
from app.event_handler import event_handler, initialize_callee_controllers
import app.event_handler

ALL_SUBREDDIT_NAME = "all"

class FrontPageController:
  def __init__(self, db_session, r, log):
    self.db_session = db_session
    self.log = log
    self.r = r
    self.all_sub = self.r.subreddit(ALL_SUBREDDIT_NAME)

    # for event_handler, a list of praw post submission objects, set in fetch_reddit_front_page
    self.posts = []

    # for event_handler, need a dictionary of {experiment id: experiment controller instance}.
    # if you forget this line, it's okay because when we run event_handler, it will look for this attr
    self.experiment_to_controller = initialize_callee_controllers(self)



  def fetch_reddit_front_page(self, pg_type, limit=100):
      posts = []
      fetched = []

      try:
        if pg_type==PageType.TOP:
            fetched = self.all_sub.top(limit=limit)
        elif pg_type==PageType.CONTR:
            fetched = self.all_sub.controversial(limit=limit)
        elif pg_type==PageType.NEW:
            fetched = self.all_sub.new(limit=limit)
        elif pg_type==PageType.HOT:
            fetched = self.all_sub.hot(limit=limit)            
      except:
        self.log.error("Error querying reddit {0} page".format(pg_type.name), extra=sys.exc_info()[0] )
        print(sys.exc.info()[0])
        return []

      for post in fetched:
          #new_post = post.json_dict if("json_dict" in dir(post)) else post['data'] ### TO HANDLE TEST FIXTURES
          new_post = thing2dict(post)
          self.posts.append(post)
          pruned_post = {
            'id': new_post['id'],
            'author': new_post['author'],
            'num_comments': new_post['num_comments'],
            'downs': new_post['downs'],
            'ups': new_post['ups'], 
            'score': new_post['score'],
            'created_utc': new_post['created_utc'],
            'subreddit_id': new_post['subreddit_id']
            }
          posts.append(pruned_post)
      self.log.info("Queried reddit {0} page".format(pg_type.name))
      return posts

  @app.event_handler.event_handler
  def archive_reddit_front_page(self, pg_type = PageType.TOP):
      posts = self.fetch_reddit_front_page(pg_type)
      front_page = FrontPage(created_at = datetime.datetime.utcnow(),
                             page_data = json.dumps(posts),
                             page_type = pg_type.value,
                             is_utc = True)
      self.db_session.add(front_page)
      try:
        self.db_session.commit()
        self.log.info("Saved reddit {0} page.".format(pg_type.name))
      except:
        self.log.error("Error while saving DB Session", extra=sys.exc_info()[0])
        print(sys.exc.info()[0])
