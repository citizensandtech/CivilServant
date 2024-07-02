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
import glob, datetime, time, pytz, math, random, copy
from app.controllers.banuser_experiment_controller import *
import app.controllers.comment_controller

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
    db_session.execute("UNLOCK TABLES")
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()
    db_session.query(Comment).delete()
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

@patch('praw.Reddit', autospec=True)
def test_initialize_experiment(mock_reddit):
    r = mock_reddit.return_value

    ## NOTE: The callback will create a new instance of 
    ##       MessagingExperimentController, so you should
    ##       not examine the state of this object for tests
    mec = BanuserExperimentController("banuser_experiment_test", db_session, r, log)
    subreddit_name = mec.experiment_settings['subreddit']
    subreddit_id = mec.experiment_settings['subreddit_id']

    ##### SETUP
    ## Set up conditions and fixtures for the comment controller
    ## which contains an event hook to run 
    ## MessagingExperimentController::enroll_new_participants
    db_session.add(Subreddit(
        id = subreddit_id, 
        name = subreddit_name))
    db_session.commit()

    comment_fixtures = []
    for filename in sorted(glob.glob("{script_dir}/fixture_data/comments*".format(script_dir=TEST_DIR))):
        f = open(filename, "r")
        comment_fixtures.append(json.loads(f.read()))
        f.close()

    m = Mock()
    m.side_effect = [comment_fixtures[0][0:100],
                     comment_fixtures[0][100:200],
                     comment_fixtures[0][200:300],
                     comment_fixtures[0][300:400],
                     comment_fixtures[0][400:500],
                     comment_fixtures[0][500:600],
                     comment_fixtures[0][600:700],
                     comment_fixtures[0][700:800],
                     comment_fixtures[0][800:900],
                     comment_fixtures[0][900:], 
                     []]

    r.get_comments = m
    patch('praw.')
    ##### END SETUP

    ## RECEIVE INCOMING COMMENTS (SUBREDDIT MATCH)
    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    assert db_session.query(Comment).count() == 0
    cc.archive_last_thousand_comments(subreddit_name)
    assert db_session.query(Comment).count() == len(comment_fixtures[0])
    
    ## NOW CHECK WHETHER THE EVENT HOOK WAS CALLED
    ## BY EXAMINING THE LOG
    file_handler = None
    for handler in log.handlers:
        if type(handler).__name__ == "ConcurrentRotatingFileHandler":
            file_handler = handler
            break

    if(file_handler):
        log_filename = handler.baseFilename
        last_log_line = None
        with open(log_filename, "r") as f:
            for line in f:
                last_log_line = line
        assert last_log_line.find("BanuserExperimentController::enroll_new_participants") > -1
    else:
        ## IF THERE'S NO CONCURRENT ROTATING FILE HANDLER
        ## RETURN FALSE
        assert False


