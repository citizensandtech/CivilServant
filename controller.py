import inspect, os, sys
import simplejson as json
import reddit.connection
import app.front_page_controller

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
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

### PROCESS POSSIBLE ACTIONS
### OPTIONS INCLUDE:
###    reddit_front: archive redditfront page
###    subreddit top: archive subreddit front page
##
##
## (TODO: process argv data more intelligently)
action = sys.argv[1]

if(action == "reddit_front"):
  r = reddit.connection.connect()
  app.front_page_controller.archive_reddit_front_page(r, db_session)
  ## TODO: log & monitor actions like this when they occur
  

