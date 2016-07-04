import re, random, string, sys, math, os, datetime
BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../", "../") 
sys.path.append(BASE_DIR)
import simplejson as json

# ONE-TIME UPDATE OF post.created to draw  from json['created_utc']
# rather than json['created']

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

print("Testing {0} posts...".format(db_session.query(Post).count()))
updated_posts = 0
for post in db_session.query(Post).all():
    post_data = json.loads(post.post_data)
    created_utc = datetime.datetime.fromtimestamp(post_data['created_utc'])
    if created_utc != post.created:
        post.created = created_utc
        updated_posts += 1
print("Updating dates for {0} posts".format(updated_posts))
db_session.commit()



