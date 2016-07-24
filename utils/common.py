from enum import Enum
import simplejson as json
from collections import namedtuple


class PageType(Enum):
    TOP = 1
    CONTR = 2 # controversial
    NEW = 3
    HOT = 4

class ThingType(Enum):
	SUBMISSION = 1
	COMMENT = 2
	SUBREDDIT = 3
	USER = 4

class DbEngine:
	def __init__(self, config_path):
		self.config_path = config_path

	def new_session(self):
		with open(self.config_path, "r") as config:
		    DBCONFIG = json.loads(config.read())

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
		return db_session  

def _json_object_hook(d):
	d['json_dict'] = d.copy()
	return namedtuple('X', d.keys(), rename=True)(*d.values())

def json2obj(data):
	return json.loads(data, object_hook=_json_object_hook)
