import inspect, os, sys
import simplejson as json
import reddit.connection
import controllers.front_page_controller
import controllers.subreddit_controller
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
r = conn.connect(controller=action)

if(action == "reddit_front_top"):
  fp = controllers.front_page_controller.FrontPageController(db_session, r, log)
  fp.archive_reddit_front_page(PageType.TOP)
  
if(action == "reddit_front_controversial"):
  fp = controllers.front_page_controller.FrontPageController(db_session, r, log)
  fp.archive_reddit_front_page(PageType.CONTR)

if(action == "reddit_front_new"):
  fp = controllers.front_page_controller.FrontPageController(db_session, r, log)
  fp.archive_reddit_front_page(PageType.NEW)

if(action == "subreddit_top"):
  subname = sys.argv[2]
  sp = controllers.subreddit_controller.SubredditPageController(subname, db_session, r, log)
  sp.archive_subreddit_page(PageType.TOP)
  
if(action == "subreddit_controversial"):
  subname = sys.argv[2]
  sp = controllers.subreddit_controller.SubredditPageController(subname, db_session, r, log)
  sp.archive_subreddit_page(PageType.CONTR)

if(action == "subreddit_new"):
  subname = sys.argv[2]
  sp = controllers.subreddit_controller.SubredditPageController(subname, db_session, r, log)
  sp.archive_subreddit_page(PageType.NEW)
