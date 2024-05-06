from enum import Enum
import contextlib
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
    MODACTION = 6

class EventWhen(Enum):
    BEFORE = 1
    AFTER = 2

class RetryableDbSession(sqlalchemy.orm.session.Session):
    # TODO Move commit logic into retryable for consistency now that it handles rollbacks

    def add_retryable(self, one_or_many, commit=True, rollback=True):
        @retryable(backoff=True, session=self, rollback=rollback)
        def _perform_add():
            try:
                self.add_all(one_or_many)
            except TypeError:
                self.add(one_or_many)
            if commit:
                self.commit()
            return one_or_many
        _perform_add()

    @retryable(backoff=False)
    def execute_retryable(self, clause, params=None, commit=True):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", r"\(1062, \"Duplicate entry")
            result = self.execute(clause, params)
            if commit:
                self.commit()
            return result

    def insert_retryable(self, model, params, commit=True, ignore_dupes=True):
        clause = model.__table__.insert()
        if ignore_dupes:
            clause = clause.prefix_with("IGNORE")
        return self.execute_retryable(clause, params, commit)
    
    def new_sibling_session(self):
        from sqlalchemy.orm import sessionmaker
        engine = self.get_bind()
        SiblingSession = sessionmaker(bind=engine, class_=RetryableDbSession)
        return SiblingSession()
    
    @contextlib.contextmanager
    def cooplock(self, resource, experiment_id):
        lock_session = self.new_sibling_session()
        from app.models import ResourceLock
        try:
            self.insert_retryable(
                ResourceLock,
                {"resource": resource, "experiment_id": experiment_id},
                ignore_dupes=True)
            query = lock_session.query(ResourceLock) \
                .with_for_update() \
                .filter_by(resource=resource, experiment_id=experiment_id)
            lock_rows = query.all()
            yield lock_session, lock_rows
            lock_session.commit()
        except:
            lock_session.rollback()
            raise
        finally:
            lock_session.close()

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
        created_utc = int(datetime.now().timestamp()) + offset
        created_utc_idx = _index_or_none(keys, 'created_utc')
        if created_utc_idx:
            values[created_utc_idx] = created_utc
        else:
            keys.append('created_utc')
            values.append(created_utc)
        dobj['json_dict']['created_utc'] = created_utc
    cls = namedtuple('HydratedTestObject', keys, rename=True)
    cls.remove = lambda x: None
    return cls(*values)

def json2obj(data, now=False, offset=0):
    object_hook = lambda dobj: _json_object_hook(dobj, now, offset)
    return json.loads(data, object_hook=object_hook)

def thing2dict(thing, recursive=True):
    # Newer versions of praw no longer store any form of their raw JSON on the
    # individual objects, so this is a workaround to retroactively produce a
    # field akin to how "data" and  "json_dict" were used previously. This
    # presumes a deep conversion is needed since individual properties may
    # themselves be praw objects.
    # e.g. when type(submission_dict['author']) == Redditor, etc.
    if not hasattr(thing, '__dict__'):
         return thing
    d = vars(thing)
    if recursive:
        from praw.models.reddit.base import RedditBase
        for k, v in d.items():
            if isinstance(v, RedditBase):
                d[k] = thing2dict(v, recursive)
    if '_reddit' in d:
        del d['_reddit']
    return d

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

