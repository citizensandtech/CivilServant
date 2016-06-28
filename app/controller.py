import inspect, os, sys
import simplejson as json
import reddit.connection
import app.controllers.front_page_controller
import app.controllers.subreddit_controller
from utils.common import PageType
import app.cs_logger

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
ENV = os.environ['CS_ENV']

with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
  DBCONFIG = json.loads(config.read())

### LOAD SQLALCHEMY SESSION
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base
db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host = DBCONFIG['host'],
    user = DBCONFIG['user'],
    password = DBCONFIG['password'],
    database = DBCONFIG['database']))

Base.metadata.bind = db_engine
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()

## LOAD LOGGER
log = app.cs_logger.get_logger(ENV, BASE_DIR)

conn = reddit.connection.Connect()

def fetch_reddit_front(page_type=PageType.TOP):
  r = conn.connect(controller="FetchRedditFront")
  fp = app.controllers.front_page_controller.FrontPageController(db_session, r, log)
  fp.archive_reddit_front_page(page_type)

def fetch_subreddit_front(sub_name, page_type = PageType.TOP):
  r = conn.connect(controller="FetchSubredditFront")
  sp = app.controllers.subreddit_controller.SubredditPageController(sub_name, db_session, r, log)
  sp.archive_subreddit_page(pg_type = page_type)
