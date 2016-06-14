import inspect, os, sys
import simplejson as json
import reddit.connection
import app.front_page_controller as front_page_controller
import app.cs_logger

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "../")
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

##########################
## METHODS FOR FETCHING ##
##########################

## Fetch Front Page of reddit
def fetch_reddit_front():
  conn = reddit.connection.Connect()
  r = conn.connect(controller="RedditFront")
  fp = front_page_controller.FrontPageController(db_session, r, log)
  fp.archive_reddit_front_page()