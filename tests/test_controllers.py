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
from app.models import Base, FrontPage, SubredditPage

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
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

def clear_front_pages():
    db_session.query(FrontPage).delete()
    db_session.commit()

def setup_function(function):
    clear_front_pages()

def teardown_function(function):
    clear_front_pages()


### TEST THE MOCK SETUP AND MAKE SURE IT WORKS
## TODO: I should not be mocking SQLAlchemy
## I should just be mocking the reddit API
@patch('praw.Reddit', autospec=True)
def test_front_page_controller(mock_reddit):
  r = mock_reddit.return_value
  with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    mock_reddit.get_top.return_value = json.loads(f.read())
  with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    mock_reddit.get_controversial.return_value = json.loads(f.read())
  patch('praw.')
  
  assert len(db_session.query(FrontPage).all()) == 0
  
<<<<<<< 60764f4b373f1b8efe04a977ca1a60ef8c6d6831
  app.front_page_controller.archive_reddit_front_page(r,db_session, PageType.TOP)
  app.front_page_controller.archive_reddit_front_page(r,db_session, PageType.CONTR)

  all_pages = db_session.query(FrontPage).all()
  assert len(all_pages) == 2
  # mmou TODO: write more assertions
=======
  ## NOW START THE TEST for top and controversial
  app.controllers.front_page_controller.archive_reddit_front_page(r,db_session, PageType.TOP)
  app.controllers.front_page_controller.archive_reddit_front_page(r,db_session, PageType.CONTR)

  all_pages = db_session.query(FrontPage).all()
  assert len(all_pages) == 2

  top_pages_count = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.TOP.value).count()
  assert top_pages_count == 1
  contr_pages_count = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.CONTR.value).count()
  assert contr_pages_count == 1


"""
### TODO: FIX BROKEN subreddit_controller.py tests
###       alembic not upgrading, tables not up to date?
###       some objects (Post?) not mocked

### TEST THE MOCK SETUP WITH AN ACTUAL QUERY
@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)    
def test_subreddit_controller(mock_subreddit, mock_reddit):

  test_subreddit_name = "science"
  test_subreddit_id = "mouw"

  r = mock_reddit.return_value
>>>>>>> add Subreddit, SubredditPage, Post tables. add code to get new and controversial front page and subreddit page posts. TODO: finish subreddit_controller tests.
  
  mock_subreddit.display_name = test_subreddit_name
  mock_subreddit.id = test_subreddit_id  
  r.get_subreddit.return_value = mock_subreddit    

  with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    mock_subreddit.get_top.return_value = json.loads(f.read())
  with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    mock_subreddit.get_controversial.return_value = json.loads(f.read())
  patch('praw.')

  assert len(db_session.query(SubredditPage).all()) == 0
  
  ## NOW START THE TEST for top and controversial  
  app.controllers.subreddit_controller.archive_subreddit_page(r,db_session, test_subreddit_name, PageType.TOP)
  app.controllers.subreddit_controller.archive_subreddit_page(r,db_session, test_subreddit_name, PageType.CONTR)

  all_pages = db_session.query(SubredditPage).all()
  assert len(all_pages) == 2

  top_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.TOP.value).count()
  assert top_pages_count == 1
  contr_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.CONTR.value).count()
  assert contr_pages_count == 1
"""