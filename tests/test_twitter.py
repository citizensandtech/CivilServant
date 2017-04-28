import app.connections.twitter_connect
import os
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
from mock import Mock, patch
from app.models import Base, PrawKey
from utils.common import DbEngine

TEST_DIR = os.path.dirname(os.path.realpath(__file__))

os.environ['CS_ENV'] ="test"

def setup_function(function):
    pass

def teardown_function(function):
    pass

# currently no tests since class TwitterConnect has no methods