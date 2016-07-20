import pytest
import os
from mock import Mock, patch
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
from utils.common import PageType, DbEngine
import socket

### LOAD THE CLASSES TO TEST
from app.models import Base, FrontPage, PrawKey

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
ENV = os.environ['CS_ENV'] ="test"

db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()

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
          print(type(FrontPage.page_type))
          front_page = FrontPage(created_at = first_item_timestamp,
                                 page_type = PageType.TOP.value,
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
    
def test_get_praw_id():
    hostname = socket.gethostname()
    assert PrawKey.get_praw_id(ENV, "DummyController") == "{0}:test:DummyController".format(hostname)    
