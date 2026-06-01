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

class DictObject(dict):
    """A dict whose keys are also attributes, with a forgiving getter."""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def _json_object_hook(dobj, now=False, offset=0):
    if now:
        from datetime import datetime
        dobj['created_utc'] = int(datetime.now().timestamp()) + offset
    return DictObject(dobj)

def json2obj(data, now=False, offset=0):
    """Parse JSON into DictObjects (key- and attribute-accessible) for use as test doubles."""
    object_hook = lambda dobj: _json_object_hook(dobj, now, offset)
    return json.loads(data, object_hook=object_hook)

# PRAW 7 Submission adds these attrs.
_PRAW_MACHINERY = {"comment_limit", "comment_sort"}

def json_dict(obj):
    """A praw-3.5 raw json_dict, reconstructed from a praw-7 object (dicts pass through)."""
    if isinstance(obj, dict):
        # Test fixtures + already-raw inputs
        return dict(obj)
    raw = vars(obj)
    d = {k: v for k, v in raw.items() if not k.startswith("_") and k not in _PRAW_MACHINERY}
    if "_mod" in raw:
        # Only ModAction shadows mod as _mod
        d["mod"] = str(obj.mod) if obj.mod is not None else None
    for key in ("author", "subreddit"):
        # Flatten nested objects to strings
        v = d.get(key)
        if v is not None and not isinstance(v, str):
            d[key] = v.name if key == "author" else str(v)
    if "author" in d and d["author"] is None:  # praw-7 deleted author -> None; 3.5 had "[deleted]"
        d["author"] = "[deleted]"
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

