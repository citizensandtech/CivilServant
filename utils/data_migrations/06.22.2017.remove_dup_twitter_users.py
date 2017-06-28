"""
dump of database prior to migration: dump_civilservant_development_2017-06-22.sql
ENV = development

for all screen_names with more than 1 record:
  merge twitter_user
  merge twitter_user_snapshot
  ...

  update lumen_notice_to_twitter_user
"""



import inspect, os, sys, copy, pytz, re, glob, time, gzip, random, csv
import simplejson as json
from collections import Counter

ENV = os.environ['CS_ENV']
BASE_DIR = "/home/reddit/cs-branches/CivilServant-mmou-twitter"
sys.path.append(BASE_DIR)


with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
  DBCONFIG = json.loads(config.read())

### LOAD SQLALCHEMY
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, and_, or_
from app.models import Base, LumenNoticeToTwitterUser, TwitterUser, TwitterUserSnapshot
from utils.common import PageType

db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host = DBCONFIG['host'],
    user = DBCONFIG['user'],
    password = DBCONFIG['password'],
    database = DBCONFIG['database']))
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()

### FILTER OUT DEPRECATION WARNINGS ASSOCIATED WITH DECORATORS
# https://github.com/ipython/ipython/issues/9242
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning, message='.*use @default decorator instead.*')

count = 0
to_delete = 0
query = "SELECT screen_name FROM twitter_users GROUP BY screen_name HAVING count(id) > 1;"
rows = db_engine.execute(text(query))


for row in rows:  # example row = ('007abdullahalm1',)
  print("...row {0}, screen_name={1}".format(count, row[0]))


  screen_name = row[0]
  records = db_session.query(TwitterUser).filter(TwitterUser.screen_name==screen_name).order_by(TwitterUser.record_created_at).all()

  seen_ids = set([])
  for record in records:
    seen_ids.add(record.id)
    seen_ids.add(record.not_found_id)
  seen_ids = list(seen_ids)


  master_record = records[0]
  for record in records[1:]:
    if "NOT_FOUND" not in record.id:
      if master_record.id is None or "NOT_FOUND" in master_record.id:
        master_record.id = record.id

        # only expect these to run once!!
        if record.created_at is not None:
          master_record.created_at = record.created_at
        if record.lang is not None:
          master_record.lang = record.lang
        if record.user_state is not None:
          master_record.user_state = record.user_state
        if record.CS_oldest_tweets_archived is not None:
          master_record.CS_oldest_tweets_archived = record.CS_oldest_tweets_archived

      else:
        print("ERROR - multiple ids for screen_name {0}: master_record.id={0}, record.id={1}".format(screen_name, master_record.id, record.id))
        sys.exit(1)
    if record.not_found_id is not None:
      master_record.not_found_id = min(record.not_found_id, master_record.not_found_id) if master_record.not_found_id is not None else record.not_found_id

    # for all records but the first, make field record_created_at=None. 
    # Prerequisite is that no records should have record_created_at=None
    # This is the hacky marker to later filter on for rows to delete.
    # All rows with record_ created_at=None should be deleted 
    record.record_created_at = None
    to_delete += 1

  nu_records = db_session.query(LumenNoticeToTwitterUser).filter(LumenNoticeToTwitterUser.twitter_username==screen_name).all()
  for nu_record in nu_records:
    nu_record.twitter_user_id = master_record.id
    CS_oldest_tweets_archived = master_record.CS_oldest_tweets_archived

  us_records = db_session.query(TwitterUserSnapshot).filter(
    or_(TwitterUserSnapshot.twitter_user_id.in_(seen_ids),
        TwitterUserSnapshot.twitter_not_found_id.in_(seen_ids)
        )).all()

  for us_record in us_records:
    us_record.twitter_user_id = master_record.id
    us_record.twitter_not_found_id = master_record.not_found_id    

  count += 1
  if count % 50 == 0:
    db_session.query(TwitterUser).filter(TwitterUser.record_created_at==None).delete()
    db_session.commit()
    print("Committed {0} updated records.".format(count))

db_session.query(TwitterUser).filter(TwitterUser.record_created_at==None).delete()
db_session.commit()
print("Finished. Updated {0} unique screen_name records. Deleted {1} rows.".format(count, to_delete))