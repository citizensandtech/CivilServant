import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from utils.common import PageType
from app.models import Base, FrontPage

ALL_SUBREDDIT_NAME = "all"

class FrontPageController:
  def __init__(self, db_session, r, log):
    self.db_session = db_session
    self.log = log
    self.all_sub = r.get_subreddit(ALL_SUBREDDIT_NAME)

  def fetch_reddit_front_page(self, pg_type, limit=100):
      posts = []
      fetched = []

      try:
        if pg_type==PageType.TOP:
            fetched = self.all_sub.get_top(limit=limit)
        elif pg_type==PageType.CONTR:
            fetched = self.all_sub.get_controversial(limit=limit)
        elif pg_type==PageType.NEW:
            fetched = self.all_sub.get_new(limit=limit)
        elif pg_type==PageType.HOT:
            fetched = self.all_sub.get_hot(limit=limit)            
      except:
        self.log.error("Error querying reddit {0} page".format(pg_type.name), extra=sys.exc_info()[0] )
        print(sys.exc.info()[0])
        return []

      for post in fetched:
          if("json_dict" in dir(post)):
              posts.append(post.json_dict)
          else:
              posts.append(post['data']) ### TO HANDLE TEST FIXTURES

      self.log.info("Queried reddit {0} page".format(pg_type.name))
      return posts

  def archive_reddit_front_page(self, pg_type = PageType.TOP):
      posts = self.fetch_reddit_front_page(pg_type)
      front_page = FrontPage(created_at = datetime.datetime.now(),
                             page_data = json.dumps(posts),
                             page_type = pg_type.value)
      self.db_session.add(front_page)
      try:
        self.db_session.commit()
        self.log.info("Saved reddit {0} page.".format(pg_type.name))
      except:
        self.log.error("Error while saving DB Session", extra=sys.exc_info()[0])
        print(sys.exc.info()[0])
