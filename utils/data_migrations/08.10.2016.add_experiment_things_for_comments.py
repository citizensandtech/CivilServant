import re, random, string, sys, math, os, datetime
BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../", "../") 
sys.path.append(BASE_DIR)
import simplejson as json
import datetime

# ONE-TIME UPDATE OF post.created to draw  from json['created_utc']
# rather than json['created']

ENV = os.environ['CS_ENV']
with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
    DBCONFIG = json.loads(config.read())

### LOAD SQLALCHEMY SESSION
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
from app.models import *
from utils.common import *
db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host = DBCONFIG['host'],
    user = DBCONFIG['user'],
    password = DBCONFIG['password'],
    database = DBCONFIG['database']))

Base.metadata.bind = db_engine
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()

print("Found {0} actions for possible conversion...".format(db_session.query(ExperimentAction).filter(and_(ExperimentAction.action=="Intervention",ExperimentAction.action_subject_id!=None)).count()))
added_comment_things = []
for action in db_session.query(ExperimentAction).filter(and_(ExperimentAction.action=="Intervention",ExperimentAction.action_subject_id!=None)).all():
  thing_exists = db_session.query(ExperimentThing).filter(ExperimentThing.id == action.action_subject_id).count() > 0 
  if(thing_exists != True):
    comment_thing = ExperimentThing(
      id = action.action_subject_id,
      experiment_id = action.experiment_id,
      object_type = ThingType.COMMENT.value,
      object_created = datetime.datetime.fromtimestamp(json.loads(action.metadata_json)['action_object_created_utc']),
      metadata_json = json.dumps({"group":"treatment", "submission_id":action.action_object_id})
    )
    added_comment_things.append(comment_thing)
    db_session.add(comment_thing)

print("Updating dates for {0} posts".format(len(added_comment_things)))
db_session.commit()
