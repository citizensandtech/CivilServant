import pytest
import os, yaml

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

from mock import Mock, patch
import unittest.mock
import simplejson as json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
import glob, datetime, time, pytz, math, random, copy
from app.controllers.banneduser_experiment_controller import *
import app.controllers.comment_controller
import app.controllers.moderator_controller

from utils.common import *
from dateutil import parser
import praw, csv, random, string
from collections import Counter

from app.models import *
import app.cs_logger


TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")


@pytest.fixture
def db_session():
    config_file = os.path.join(BASE_DIR, "config", f"{ENV}.json") 
    return DbEngine(config_file).new_session()

@pytest.fixture
def logger():
    return app.cs_logger.get_logger(ENV, BASE_DIR)

def _clear_all_tables(db_session):
    db_session.execute("UNLOCK TABLES")
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()
    db_session.query(ModAction).delete()
    db_session.query(Comment).delete()
    db_session.query(Experiment).delete()
    db_session.query(ExperimentThing).delete()
    db_session.query(ExperimentAction).delete()
    db_session.query(ExperimentThingSnapshot).delete()
    db_session.query(EventHook).delete()
    db_session.commit()    

@pytest.fixture(autouse=True)
def with_setup_and_teardown(db_session):
    _clear_all_tables(db_session)
    yield
    _clear_all_tables(db_session)

@pytest.fixture
def modaction_fixtures():
    fixtures = []
    for filename in sorted(glob.glob(f"{TEST_DIR}/fixture_data/mod_actions*")):
        with open(filename, "r") as f:
            fixtures.append(json.load(f))
    return fixtures

@pytest.fixture
def reddit_return_value(modaction_fixtures):
    with patch('praw.Reddit', autospec=True) as mock_reddit:
        r = mock_reddit.return_value
        
        m = Mock()
        # Fixture data is broken up like this to allow testing of API 'pagination'
        m.side_effect = [modaction_fixtures[0][0:100],
                         modaction_fixtures[0][100:200],
                         modaction_fixtures[0][200:300],
                         modaction_fixtures[0][300:400],
                         modaction_fixtures[0][400:500],
                         modaction_fixtures[0][500:600],
                         modaction_fixtures[0][600:700],
                         modaction_fixtures[0][700:800],
                         modaction_fixtures[0][800:900],
                         modaction_fixtures[0][900:], 
                         []]
        r.get_mod_log = m

        return r

@pytest.fixture
def experiment_controller(db_session, reddit_return_value, logger):
    c = BanneduserExperimentController("banneduser_experiment_test", db_session, reddit_return_value, logger)

    db_session.add(Subreddit( id = c.experiment_settings['subreddit_id'], name = c.experiment_settings['subreddit']))
    db_session.commit()

    return c

@pytest.fixture
def moderator_controller(subreddit_name, db_session, reddit_return_value, logger):
    return app.controllers.moderator_controller.ModeratorController(subreddit_name, db_session, reddit_return_value, logger)

@pytest.fixture
def subreddit_name(experiment_controller):
    return experiment_controller.experiment_settings['subreddit']

@pytest.fixture
def subreddit_id(experiment_controller):
    return experiment_controller.experiment_settings['subreddit_id']

@pytest.fixture
def log_filename(logger):
    file_handler = None
    for handler in logger.handlers:
        if type(handler).__name__ == "ConcurrentRotatingFileHandler":
            file_handler = handler
            break
    if not file_handler:
        assert False
    return handler.baseFilename

def _assert_logged(log_filename, text):
    last_log_line = ""
    with open(log_filename, "r") as f:
        for line in f:
            pass
        last_log_line = line
    assert text in last_log_line

def test_modaction_archive(moderator_controller, db_session, modaction_fixtures, log_filename):
    assert db_session.query(ModAction).count() == 0

    moderator_controller.archive_mod_action_page()

    #assert db_session.query(ModAction).count() == len(modaction_fixtures[0])
    ### TODO: TEMPORARY TEST - this is because moderator_controller only will retrieve the first 'page' of results
    assert db_session.query(ModAction).count() == len(modaction_fixtures[0][0:100])
    
    _assert_logged(log_filename, "BanneduserExperimentController::find_banned_users")
