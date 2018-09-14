# coding: utf-8

# # Goals
# ## Pre experiment
# + delete all lumen/ twitter tables
# 	+ ensure counts of all tables are zero
# + check experiment lenght
#
# ## Post experiment
# + statistics on twitter and lumen tables
# + ensure onboarding period and collection period are staggered correctly
# + calculate naive tweets per day for users

import inspect, os, sys
### LOAD ENVIRONMENT VARIABLES
import json

from common import DbEngine

import click

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
sys.path.append(BASE_DIR)
ENV = os.environ['CS_ENV']

with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
    DBCONFIG = json.load(config)

### LOAD SQLALCHEMY
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host=DBCONFIG['host'],
    user=DBCONFIG['user'],
    password=DBCONFIG['password'],
    database=DBCONFIG['database']))
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()

from app.models import LumenNotice, LumenNoticeExpandedURL, LumenNoticeToTwitterUser
from app.models import TwitterUserState, TwitterRateState, TwitterStatus, TwitterToken, TwitterUser, TwitterUserSnapshot

tables = [LumenNotice, LumenNoticeToTwitterUser, LumenNoticeExpandedURL,
          TwitterUserSnapshot, TwitterUser, TwitterToken, TwitterStatus, TwitterRateState]


def count_table(table):
    return db_session.query(table).count()

def count_all_tables():
    table_counts = {table.__tablename__: count_table(table) for table in tables}
    print(table_counts)
    # return if all tables are 0
    return all([count == 0 for table, count in table_counts.items()])


def pre_experiment():

    def del_tables():
        for table in tables:
            table_name = table.__tablename__
            truncate_sql = 'truncate {} ;'.format(table_name)
            print(truncate_sql)
            db_session.execute(truncate_sql)
            db_session.commit()

    count_table(TwitterRateState)
    count_all_tables()
    del_tables()

def post_experiment():
    raise NotImplementedError

@click.command()
@click.option('--when', type=click.Choice(['md5', 'sha1']))
def main(when):
    if when == 'pre':
        return pre_experiment()
    elif when == 'post':
        return post_experiment()

if __name__ == "__main__":
    main()
