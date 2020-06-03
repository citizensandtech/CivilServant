from enum import Enum
from operator import eq, not_

import simplejson as json
import sqlalchemy.orm.session
import warnings
from collections import namedtuple
import datetime

NOT_FOUND_TWITTER_USER_STR = "<NOT_FOUND>"
LOGS_DIR = str(pathlib.Path(BASE_DIR, "logs"))
pathlib.Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

EXPERIMENT_LANGUAGES = ['en', 'en-gb', 'en-GB']

TWITTER_STRPTIME = '%a %b %d %H:%M:%S %z %Y'

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

# not formalized...
class TwitterUserState(Enum):
    FOUND = 1
    NOT_FOUND = 2 # deleted (or never existed)
    SUSPENDED = 3
    PROTECTED = 4
    NOT_QUALIFYING = 5

class TwitterUserCreateType(Enum):
    LUMEN_NOTICE = 1
    RANDOMLY_GENERATED = 2

# for model fields CS_*
class CS_JobState(Enum):
    NOT_PROCESSED = 1   # haven't run yet; need to run
    IN_PROGRESS = 2     # also means it was at least attempted
    PROCESSED = 3       # finished running
    FAILED = 4          # in_progress but never succeeded processing; because e.g. internet went down or system crashed
    NEEDS_RETRY = 5     # for flagging purposes...
    WONT_PROCESS = 6    # decide not process because they were never existing in the first place.

class TwitterUrlKey(Enum):
    ENTITY = 1
    EXTENDED = 2 # extended entity, aka media
    RETWEETED_ENTITY = 3
    RETWEETED_EXTENDED = 4
    QUOTED_ENTITY = 5
    QUOTED_EXTENDED = 6

def generate_not_found_twitter_user_id(screen_name=""):
    capped_screen_name = screen_name if len(screen_name)<30 else screen_name[:30] + "..."
    return "{0}_{1}_{2}".format(
        NOT_FOUND_TWITTER_USER_STR,
        capped_screen_name,
        time_since_epoch_ms(datetime.datetime.utcnow())
        )

def update_all_CS_JobState(row_to_state, field, db_session, log):
    if len(row_to_state) == 0:
        log.info("Updated 0 CS_JobState fields.")
        return

    for row in row_to_state:
        setattr(row, field, row_to_state[row].value)

    try:
        db_session.commit()
        log.info("Updated {0} {1} {2} fields to new CS_JobState.".format(len(row_to_state), type(list(row_to_state.keys())[0]), field))
    except:
        log.error("Error while saving DB Session for updating {0} {1} {2} fields to new CS_JobState.".format(len(row_to_state), type(list(row_to_state.keys())[0]), field), extra=sys.exc_info()[0])


def update_CS_JobState(rows, field, to_state, db_session, log):
    if len(rows) == 0:
        log.info("Updated 0 CS_JobState fields.")
        return

    for row in rows:
        setattr(row, field, to_state.value)
    try:
        db_session.commit()
        log.info("Updated {0} {1} {2} fields to {3}.".format(len(rows), type(rows[0]), field, to_state))
    except:
        log.error("Error while saving DB Session for updating {0} {1} {2} fields to {3}.".format(len(rows), type(rows[0]), field, to_state), extra=sys.exc_info()[0])

def reset_CS_JobState_In_Progress(rows, field, db_session, log):
    if len(rows) == 0:
        log.info("Updated 0 CS_JobState fields.")
        return

    changed_rows = []
    for row in rows:
        if getattr(row, field) == CS_JobState.IN_PROGRESS.value:
            setattr(row, field, CS_JobState.NOT_PROCESSED.value)
            changed_rows.append(row)

    try:
        db_session.commit()
        log.info("Updated {0} {1} {2} fields to CS_JobState NOT_PROCESSED.".format(len(changed_rows), type(rows[0]), field))
    except:
        log.error("Error while saving DB Session for updating {0} {1} {2} fields to CS_JobState NOT_PROCESSED.".format(len(changed_rows), type(rows[0]), field), extra=sys.exc_info()[0])


class ParseUsernameSuspendedUserFound(Exception):
    pass


class EventWhen(Enum):
    BEFORE = 1
    AFTER = 2

class RetryableDbSession(sqlalchemy.orm.session.Session):
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
        db_engine = create_engine("mysql://{user}:{password}@{host}/{database}?charset=utf8mb4".format(
		    host = DBCONFIG['host'],
		    user = DBCONFIG['user'],
		    password = DBCONFIG['password'],
            database = DBCONFIG['database']), pool_recycle=3600, encoding='utf8')

		Base.metadata.bind = db_engine
        DBSession = sessionmaker(bind=db_engine)
		db_session = DBSession()
		return db_session

def _json_object_hook(dobj):
	dobj['json_dict'] = dobj.copy()
	X =  namedtuple('X', dobj.keys(), rename=True)
	X.remove = lambda x: None
	return(X(*dobj.values()))

def json2obj(data):
	return json.loads(data, object_hook=_json_object_hook)


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


def time_since_epoch_ms(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)


def neq(x, y):
    """
    implementation of not equals for SQLalchemy ORM
    :param sqlalchemy column:
    :param sqlalchemy column:
    :return not-equals x, y:
    """
    return not_(eq(x, y))
