import pytest
import os, yaml

## SET UP THE DATABASE ENGINE
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] = "test"

from mock import Mock, patch
import unittest.mock
import simplejson as json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
import glob, datetime, time, pytz
from app.controllers.mod_user_experiment_controller import *
from app.controllers.front_page_controller import FrontPageController

from utils.common import *
from dateutil import parser
import praw, csv, random, string
from collections import Counter

### LOAD THE CLASSES TO TEST
from app.models import *
import app.cs_logger


db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)

def clear_all_tables():
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()
    db_session.query(Comment).delete()
    db_session.query(ModAction).delete()
    db_session.query(Experiment).delete()
    db_session.query(ExperimentThing).delete()
    db_session.query(ExperimentAction).delete()
    db_session.query(ExperimentThingSnapshot).delete()
    db_session.query(EventHook).delete()
    db_session.commit()    

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()


def setup_banned_user_fixtures(r):
    experiment_name = "mod_user_test"
    with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
            experiment_config = yaml.load(f)['test']

    controller = ModeratorExperimentController(experiment_name, db_session, r, log)

    mod_action_fixtures = []
    for filename in sorted(glob.glob("{script_dir}/fixture_data/mod_action*".format(script_dir=TEST_DIR))):
        f = open(filename, "r")
        mod_action_list = []
        for mod_action in json.loads(f.read()):
            mod_action['sr_id36'] = experiment_config['subreddit_id']
            mod_action_list.append(json2obj(json.dumps(mod_action)))
        mod_action_fixtures.append(mod_action_list)
        f.close()

    r.get_subreddit.return_value = json2obj('{{"name":"{0}", "id":"{1}", "display_name":"{2}"}}'.format(experiment_config['subreddit'], experiment_config['subreddit_id'], experiment_config['subreddit']))
    r.get_mod_log.return_value = mod_action_fixtures[0] #contains two banuser records

    return controller, mod_action_fixtures
    
    #controller.query_and_archive_banned_users_main()
    ## TEST THAT THE PROPER NUMBER OF ACTIONS ARE ADDED TO THE DATABASE
    #assert db_session.query(ModAction).count() == len(mod_action_fixtures[0])

    #assert len(controller.get_banned_users(controller.subreddit_id, None)) == 2

##TODO FOR TESTS

## TEST THE pre function, which archives the main subreddit
##      query_and_archive_banned_users_main
##      Fixtures and mocks:
#       - past mod log for main
#         - test cases where the accounts are older than oldest_mod_action_created_utc
#         - test creation of new user metadata records
#         - cases where we can get info about the user
#         - cases where the user is not queryable from the reddit api because it's been removed from the system
#       - more recent updates to main mod log
#       - mod actions in shadow subreddit
@patch('praw.Reddit', autospec=True)
def test_query_and_archive_banned_users_main(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')
    controller, mod_action_fixtures = setup_banned_user_fixtures(r)
    controller.query_and_archive_banned_users_main()
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0])
    #assert len(controller.get_banned_users(controller.subreddit_id, None)) == 2
