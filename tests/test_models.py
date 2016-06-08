import pytest
import os
from mock import Mock, patch
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime

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

@pytest.fixture
def populate_front_pages():
    fixture_dir = os.path.join(TEST_DIR, "fixture_data")
    counter = 0
    for fn in glob.glob(fixture_dir + "/front_page_*.json"):
        with open(fn) as front_page_file:
          front_page_data = json.loads(front_page_file.read())['data']['children']
          first_item_timestamp = datetime.datetime.fromtimestamp(front_page_data[0]['data']['created'])
          
          front_page = FrontPage(created_at = first_item_timestamp,
                                 page_data = json.dumps(front_page_data))
          db_session.add(front_page)
        counter += 1
    db_session.commit()
    return counter


### TEST THE MOCK SETUP AND MAKE SURE IT WORKS
def test_front_page(populate_front_pages):
    all_pages = db_session.query(FrontPage).all()
    assert len(all_pages) == 3
    assert len(json.loads(all_pages[0].page_data)) == 100
    
    
    
  
