"""

ONE-TIME UPDATE OF converting SubredditPage.created_at, FrontPage.created_at to utc

Daylight Savings Time = Nov 6 2am

if we see the ET time Nov 6 12am, then it is EDT: EDT-->UTC = +4 = Nov 6 4am

if we see the ET time Nov 6 1-2am, then it is unclear whether it is EDT or EST; assume it is EST
	> assumption is becaues I don't think we really care about this, as long as we are consistent

if we see the ET time Nov 6 2:30am, then it is EST: EST-->UTC = +5 = Nov 6 7:30am 
if we see the ET time Nov 6 3am, then it is EST: EST-->UTC = +5 = Nov 6 8am 

"""

import re, random, string, sys, math, os, datetime
BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../", "../") 
sys.path.append(BASE_DIR)
import simplejson as json

ENV = os.environ['CS_ENV']
with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
    DBCONFIG = json.loads(config.read())

### LOAD SQLALCHEMY SESSION
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base, SubredditPage, FrontPage
db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host = DBCONFIG['host'],
    user = DBCONFIG['user'],
    password = DBCONFIG['password'],
    database = DBCONFIG['database']))

Base.metadata.bind = db_engine
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()


#################

DST_LIMIT_ET = datetime.datetime(2016, 11, 6, 1, 00)	# year, month, day, hour, second
EDT_TO_UTC = datetime.timedelta(hours=4) # +4 hours; EDT = in DST; before Nov 6 1am
EST_TO_UTC = datetime.timedelta(hours=5) # +5 hours; EST = not in DST; after Nov 6 1am

for model in [SubredditPage, FrontPage]:
	posts = db_session.query(model)
	total_posts = posts.count()
	num_updated_posts = 0
	last_et_time_utc = datetime.datetime.min
	last_edt_time = datetime.datetime.min
	num_confusing_et_times = 0
	print("Testing {0} posts...".format(total_posts))
	for post in posts.all():
		if not post.is_utc:
			created_at_et = post.created_at
			if created_at_et < DST_LIMIT_ET:	
				# is EDT; in DST; before Nov 6 2am = Daylight Savings Time
				created_at_utc = created_at_et - EDT_TO_UTC 
				last_edt_time = max([last_edt_time, created_at_et])
			else:	
				# is EST; out of DST
				if created_at_et < DST_LIMIT_ET + datetime.timedelta(hours=1):
					# if between 1am and 2am on Nov 6
					num_confusing_et_times += 1
				created_at_utc = created_at_et - EST_TO_UTC
			post.created_at = created_at_utc
			post.is_utc = True
			num_updated_posts += 1
			last_et_time_utc = max([last_et_time_utc, created_at_utc])
	print("Updating created_at for {0} posts; updated created_at to UTC up to time {1}; DST found up to time {2}; num_confusing_et_times: {3}".format(num_updated_posts, last_et_time_utc, last_edt_time, num_confusing_et_times))
	db_session.commit()



