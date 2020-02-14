from enum import Enum
import pathlib
import simplejson as json
import sqlalchemy.orm.session
import warnings
from collections import namedtuple
from utils.retry import retryable

BASE_DIR = str(pathlib.Path(__file__).parents[1])
LOGS_DIR = str(pathlib.Path(BASE_DIR, "logs"))
pathlib.Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

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
	STYLESHEET = 5

class EventWhen(Enum):
    BEFORE = 1
    AFTER = 2

class RetryableDbSession(sqlalchemy.orm.session.Session):
    @retryable(backoff=True)
    def add_retryable(self, one_or_many, commit=True):
        try:
            self.add_all(one_or_many)
        except TypeError:
            self.add(one_or_many)
        if commit:
            self.commit()
        return one_or_many

    @retryable(backoff=True)
    def execute_retryable(self, clause, params=None, commit=True):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", r"\(1062, \"Duplicate entry")
            self.execute(clause, params)
            if commit:
                self.commit()

    def insert_retryable(self, model, params, commit=True, ignore_dupes=True):
        clause = model.__table__.insert()
        if ignore_dupes:
            clause = clause.prefix_with("IGNORE")
        self.execute_retryable(clause, params, commit)

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
		    database = DBCONFIG['database']), pool_recycle=3600)

		Base.metadata.bind = db_engine
		DBSession = sessionmaker(bind=db_engine, class_=RetryableDbSession)
		db_session = DBSession()
		return db_session

def _index_or_none(l, obj):
    try:
        return l.index(obj)
    except ValueError:
        return None

def _json_object_hook(dobj, now=False, offset=0):
    dobj['json_dict'] = dobj.copy()
    keys = list(dobj.keys())
    values = list(dobj.values())
    if now:
        from datetime import datetime
        created_utc = int(datetime.utcnow().timestamp()) + offset
        created_utc_idx = _index_or_none(keys, 'created_utc')
        if created_utc_idx:
            values[created_utc_idx] = created_utc
        else:
            keys.append('created_utc')
            values.append(created_utc)
        dobj['json_dict']['created_utc'] = created_utc
    Hydrated = namedtuple('Hydrated', keys, rename=True)
    Hydrated.remove = lambda x: None
    return Hydrated(*values)

def json2obj(data, now=False, offset=0):
    object_hook = lambda dobj: _json_object_hook(dobj, now, offset)
    return json.loads(data, object_hook=object_hook)

class CommentNode:
	def __init__(self, id, data, link_id = None, toplevel = False, parent=None):
		self.id = id
		self.children = list()
		self.parent = parent
		self.link_id = link_id
		self.toplevel = toplevel
		self.data = data

	def add_child(self, child):
		self.children.append(child)

	def set_parent(self,parent):
		self.parent = parent

	def get_all_children(self):
		all_children = self.children
		for child in self.children:
			all_children = all_children + child.get_all_children()
		if(len(all_children)>0):
			return all_children
		else:
			return []

	def __str__(self):
		return str(self.id)

