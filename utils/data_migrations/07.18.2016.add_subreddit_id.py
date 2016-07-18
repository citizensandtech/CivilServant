import re, random, string, sys, math, os, datetime
BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../", "../") 
sys.path.append(BASE_DIR)
import simplejson as json
from app.models import SubredditPage, FrontPage


"""
ONE-TIME UPDATE for post-filling in subreddit id for rows in subreddit_pages. 
apply this migration after alembic upgrading to 2957ac0e11b6_add_subreddit_id_field_to_subreddit_.py.
subreddit id should be 'mouw' (for r/science) (note that we strip the prefix 't5_')
"""

### LOAD SQLALCHEMY SESSION
ENV = os.environ['CS_ENV']
with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
    DBCONFIG = json.loads(config.read())

### LOAD SQLALCHEMY SESSION
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base, SubredditPage, Subreddit, Post, ModAction
db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host = DBCONFIG['host'],
    user = DBCONFIG['user'],
    password = DBCONFIG['password'],
    database = DBCONFIG['database']))

Base.metadata.bind = db_engine
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()


### START UPDATES
rscience_id = "mow"

print("Testing {0} pages...".format(db_session.query(SubredditPage).count()))
updated_pages = 0
for page in db_session.query(SubredditPage).all():
    page.subreddit_id = rscience_id
    updated_pages += 1
print("Updating subreddit_id for {0} pages".format(updated_pages))
db_session.commit()