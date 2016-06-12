import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries


from app.models import Base, FrontPage

class FrontPageController:
  def __init__(self, db_session, r, log):
    self.db_session = db_session
    self.log = log
    self.r = r

  def fetch_top_reddit_front_page(self, limit=100):
      top_posts = []
      try:
        top = self.r.get_top(limit = limit)
      except:
        self.log.error("Error querying reddit top page", extra=sys.exc_info()[0] )
        print(sys.exc.info()[0])
        return []

      for post in self.r.get_top(limit=limit):
          if("json_dict" in dir(post)):
              top_posts.append(post.json_dict)
          else:
              top_posts.append(post) ### TO HANDLE TEST FIXTURES

      self.log.info("Queried reddit top page")
      return top_posts

  def archive_reddit_front_page(self):
      top_posts = self.fetch_top_reddit_front_page()
      front_page = FrontPage(created_at = datetime.datetime.now(),
                             page_data = json.dumps(top_posts))
      self.db_session.add(front_page)
      try:
        self.db_session.commit()
        self.log.info("Saved reddit front page.")
      except:
        self.log.error("Error while saving DB Session", extra=sys.exc_info()[0])
        print(sys.exc.info()[0])
