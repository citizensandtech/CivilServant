import inspect, os, sys, copy, pytz, re, glob, time, gzip, random, csv
import simplejson as json
from collections import Counter

ENV = os.environ['CS_ENV']
BASE_DIR = "~/CivilServant"
sys.path.append(BASE_DIR)

migrationdata_path = "~/CivilServant-backups/migrations"


with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
  DBCONFIG = json.loads(config.read())

### LOAD SQLALCHEMY
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, and_, or_
from app.models import Base, SubredditPage, FrontPage, Subreddit, Post, ModAction, Experiment
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

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

for subreddit in [{"table":"subreddit_page_utc_ids",
                   "file":"03.30.2017.subreddit_page.is_utc.nulls.csv"},
                  {"table":"front_page_utc_ids",
                   "file":"03.30.2017.front_page.is_utc.nulls.csv"}]:
    
    
    all_rows = []
    print("\nProcessing {0}".format(subreddit['file']))

    with open(os.path.join(migrationdata_path, subreddit['file'])) as f:
        for row in csv.DictReader(f):
            all_rows.append(row)

    for row in batch(all_rows, 20000):
        query = "INSERT INTO {0}(id) VALUES({1});".format(subreddit['table'],
          "),(".join([a['id'] for a in row]))
        result = db_engine.execute(text(query))
        if(len(row) == result.rowcount):
            sys.stdout.write(".")
        else:
            sys.stdout.write("x")
#            import pdb;pdb.set_trace()
#            break


