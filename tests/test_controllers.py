import pytest
import os
from mock import Mock, patch
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
import app.controllers.front_page_controller
import app.controllers.subreddit_controller
from utils.common import PageType

### LOAD THE CLASSES TO TEST
from app.models import Base, FrontPage, SubredditPage, Subreddit, Post, User
import app.cs_logger

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] ="test"
with open(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV), "r") as config:
  DBCONFIG = json.loads(config.read())

db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host = DBCONFIG['host'],
    user = DBCONFIG['user'],
    password = DBCONFIG['password'],
    database = DBCONFIG['database']))
    
Base.metadata.bind = db_engine
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()


def setup_function(function):
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()    
    db_session.commit()    

def teardown_function(function):
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()    
    db_session.commit()


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)    
def test_archive_reddit_front_page(mock_subreddit, mock_reddit):
  ### TEST THE MOCK SETUP AND MAKE SURE IT WORKS
  ## TODO: I should not be mocking SQLAlchemy
  ## I should just be mocking the reddit API

  r = mock_reddit.return_value
  log = app.cs_logger.get_logger(ENV, BASE_DIR)

  with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    sub_data = json.loads(f.read())['data']['children']
  mock_subreddit.get_top.return_value = sub_data
  mock_subreddit.get_controversial.return_value = sub_data
  mock_subreddit.get_new.return_value = sub_data
  mock_subreddit.get_hot.return_value = sub_data  
  patch('praw.')

  r.get_subreddit.return_value = mock_subreddit   

  
  assert len(db_session.query(FrontPage).all()) == 0
  
  ## NOW START THE TEST for top, controversial, new
  fp = app.controllers.front_page_controller.FrontPageController(db_session, r, log)
  fp.archive_reddit_front_page(PageType.TOP)
  fp.archive_reddit_front_page(PageType.CONTR)
  fp.archive_reddit_front_page(PageType.NEW)
  fp.archive_reddit_front_page(PageType.HOT)  

  all_pages = db_session.query(FrontPage).all()
  assert len(all_pages) == 4

  top_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.TOP.value)
  assert top_pages.count() == 1

  contr_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.CONTR.value)
  assert contr_pages.count() == 1

  new_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.NEW.value)
  assert new_pages.count() == 1

  new_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.HOT.value)
  assert new_pages.count() == 1  


"""
  basic test for method archive_subreddit_page to insert timestamped pages to subreddit_pages table.
  analogous to test_archive_reddit_front_page.
"""
@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)    
def test_archive_subreddit_page(mock_subreddit, mock_reddit):
  ### TODO: TEST THE MOCK SETUP WITH AN ACTUAL QUERY

  test_subreddit_name = "science"
  test_subreddit_id = "mouw"

  r = mock_reddit.return_value
  log = app.cs_logger.get_logger(ENV, BASE_DIR)

  with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    sub_data = json.loads(f.read())['data']['children']
  mock_subreddit.get_top.return_value = sub_data
  mock_subreddit.get_controversial.return_value = sub_data
  mock_subreddit.get_new.return_value = sub_data
  mock_subreddit.get_hot.return_value = sub_data  
  patch('praw.')

  mock_subreddit.display_name = test_subreddit_name
  mock_subreddit.id = test_subreddit_id  
  r.get_subreddit.return_value = mock_subreddit    

  assert len(db_session.query(SubredditPage).all()) == 0
  sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)  

  ## NOW START THE TEST for top, controversial, new  
  sp.archive_subreddit_page(PageType.TOP)
  sp.archive_subreddit_page(PageType.CONTR)
  sp.archive_subreddit_page(PageType.NEW)
  sp.archive_subreddit_page(PageType.HOT)

  all_pages = db_session.query(SubredditPage).all()
  assert len(all_pages) == 4

  top_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.TOP.value).count()
  assert top_pages_count == 1

  contr_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.CONTR.value).count()
  assert contr_pages_count == 1

  new_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.NEW.value).count()
  assert new_pages_count == 1

  hot_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.HOT.value).count()
  assert hot_pages_count == 1


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)    
def test_archive_subreddit(mock_subreddit, mock_reddit):
  test_subreddit_name = "science"
  test_subreddit_id = "mouw"

  r = mock_reddit.return_value
  log = app.cs_logger.get_logger(ENV, BASE_DIR)

  mock_subreddit.display_name = test_subreddit_name
  mock_subreddit.id = test_subreddit_id  
  patch('praw.')

  assert len(db_session.query(Subreddit).all()) == 0
  sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)  

  ## NOW START THE TEST
  # TODO: should you even be allowed to archive a different subreddit than the one sp was made for?  
  sp.archive_subreddit(mock_subreddit)

  all_subs = db_session.query(Subreddit).all()
  assert len(all_subs) == 1

  ## trying to archive it again should do nothing (don't throw errors, don't edit db)
  sp.archive_subreddit(mock_subreddit)

  all_subs = db_session.query(Subreddit).all()
  assert len(all_subs) == 1

@patch('praw.Reddit', autospec=True)
def test_archive_post(mock_reddit):

  # dummy post just to pass the test. 
  # TODO: carefully describe what the types of these 'archive' method args should be...
  post = {
      'id': 1, 
      'subreddit_id': 't5_mouw', 
      'created': 1356244946.0
  }

  r = mock_reddit.return_value
  test_subreddit_name = "science"
  log = app.cs_logger.get_logger(ENV, BASE_DIR)
  patch('praw.')

  assert len(db_session.query(Post).all()) == 0
  sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)  

  ## NOW START THE TEST
  sp.archive_post(post)

  all_posts = db_session.query(Post).all()
  assert len(all_posts) == 1

  ## trying to archive it again should do nothing (don't throw errors, don't edit db)
  sp.archive_post(post)

  all_posts = db_session.query(Post).all()
  assert len(all_posts) == 1


@patch('praw.Reddit', autospec=True)
def test_archive_user(mock_reddit):

  username = "merrymou"
  seen_at = datetime.datetime.now()

  r = mock_reddit.return_value
  test_subreddit_name = "science"
  log = app.cs_logger.get_logger(ENV, BASE_DIR)
  patch('praw.')

  assert len(db_session.query(User).all()) == 0
  sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)  

  ## NOW START THE TEST
  sp.archive_user(username, seen_at)

  all_users = db_session.query(User).all()
  assert len(all_users) == 1

  user = db_session.query(User).first()  
  old_last_seen = user.last_seen

  ## trying to archive it again should update last_seen field
  sp.archive_user(username, seen_at)

  all_users = db_session.query(User).all()
  assert len(all_users) == 1  

  user = db_session.query(User).first()  
  new_last_seen = user.last_seen
  assert(old_last_seen <= new_last_seen)
