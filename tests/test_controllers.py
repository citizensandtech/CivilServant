import pytest
import os
from mock import Mock, patch
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
import app.front_page_controller

### LOAD THE CLASSES TO TEST
from app.models import Base, FrontPage

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
    mock_reddit.get_new.return_value = json.loads(f.read())
  patch('praw.')
  
  assert len(db_session.query(FrontPage).all()) == 0
  
  app.front_page_controller.archive_reddit_front_page(r,db_session)

  all_pages = db_session.query(FrontPage).all()
  assert len(all_pages) == 1
    
    
  
