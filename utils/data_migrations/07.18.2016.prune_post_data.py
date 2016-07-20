import re, random, string, sys, math, os, datetime
BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../", "../") 
sys.path.append(BASE_DIR)
import simplejson as json
from app.models import SubredditPage, FrontPage


"""
ONE-TIME UPDATE OF page_data to only contain the following fields

id
author
num_comments
downs
ups
score

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
tables = [FrontPage, SubredditPage]

for table in tables:
    print("Testing {0} {1} pages...".format(db_session.query(table).count(), table.__tablename__))
    updated_pages = 0 
    for page in db_session.query(table).all():
        posts = json.loads(page.page_data)
        pruned = [{'id': post['id'], 'author': post['author'], 'num_comments': post['num_comments'], 'downs': post['downs'], 'ups': post['ups'], 'score': post['score']} for post in posts]
        page.page_data = json.dumps(pruned)
        updated_pages += 1
    print("Updating page_data for {0} {1} pages".format(updated_pages, table.__tablename__))
    db_session.commit()
