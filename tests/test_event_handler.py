"""
EventHandler tests also illustrate how the EventHandler works and how to use it.

First 2 tests are for a dummy callee/caller example.

"""

import pytest
import os


## SET UP THE DATABASE ENGINE
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] = "test"


from mock import Mock, patch

import yaml
import praw
import simplejson as json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
import glob, datetime, time, pytz

from app.event_handler import event_handler, initialize_callee_controllers
from app.controllers.sticky_comment_experiment_controller import StickyCommentExperimentController # import * ?
from utils.common import *

### LOAD THE CLASSES TO TEST
from app.models import *
import app.cs_logger


db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)

def clear_all_tables():
    db_session.query(EventHook).delete()
    db_session.query(Experiment).delete()
    db_session.query(ExperimentThing).delete()
    db_session.query(ExperimentAction).delete()
    db_session.query(ExperimentThingSnapshot).delete()
    db_session.commit()    

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()


####################################
#########   CALLEE     #############
####################################

class StickyCommentExperimentTestController(StickyCommentExperimentController):
    def __init__(self, experiment_name, db_session, r, log):
        required_keys = ['subreddit', 'subreddit_id', 'username', 
                         'start_time', 'end_time',
                         'max_eligibility_age', 'min_eligibility_age',
                         'conditions', 'event_hooks']

        super().__init__(experiment_name, db_session, r, log, required_keys)

    """
        callback methods must pass in these 2 arguments: 
            self: an instance of callee class
            instance: an instance of caller class
    """
    def test_before_hook(self, instance):
        assert(instance.num_callbacks_run == 0)
        instance.num_callbacks_run += 1
        assert(self.experiment_name == EXPERIMENT_NAME)
        assert(instance.data is None)

    def test_before_inactive_hook(self, instance):
        assert(False)   # should never get here

    def test_after_hook(self, instance):
        instance.num_callbacks_run += 1        
        assert(self.experiment_name == EXPERIMENT_NAME)
        assert(instance.data is DATA_TEXT)

    def test_after_count_hook(self, instance):
        instance.num_callbacks_run += 1        
        assert(self.experiment_name == EXPERIMENT_NAME)
        assert(instance.data is DATA_TEXT)

####################################
#########   CALLER     #############
####################################


class SomeTestController:

    def __init__(self, db_session, r, log):
        self.db_session = db_session
        self.log = log 
        self.r = r

        # all variables that are passed between callee and caller instances must be class attributes
        self.data = None
        self.num_callbacks_run = 0

        # for event_handler, need a dictionary of {experiment id: experiment controller instance}.
        # if you forget this line, it's okay because when we run event_handler, it will look for this attr
        self.experiment_to_controller = initialize_callee_controllers(self)

    """
        sets self.data to be DATA_TEXT
    """
    @event_handler   
    def test_set_data(self):
        assert(self.data is None)
        self.data = DATA_TEXT
        return self.data


####################################
#########   TESTS      #############
####################################


DATA_TEXT = "HERE'S THE DATA!!!"
EXPERIMENT_NAME = "sticky_comment_event_handler_test"


# initialize experiment, load hooks
@patch('praw.Reddit', autospec=True)
def test_initialize_experiment(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    test_experiment_name = EXPERIMENT_NAME

    assert(len(db_session.query(Experiment).all()) == 0)
    StickyCommentExperimentTestController(test_experiment_name, db_session, r, log)

    assert(len(db_session.query(Experiment).all()) == 1)
    hooks = db_session.query(EventHook).all()
    assert(len(hooks) == 4)

    before_events = db_session.query(EventHook).filter(EventHook.call_when == EventWhen.BEFORE.value).all()
    assert(len(before_events) == 2)
    before_active_events = db_session.query(EventHook).filter(and_(
        EventHook.call_when == EventWhen.BEFORE.value,
        EventHook.is_active == True)).all()
    assert(len(before_active_events) == 1)
    after_events = db_session.query(EventHook).filter(EventHook.call_when == EventWhen.AFTER.value).all()
    assert(len(after_events) == 2)

# test that event handler runs the callbacks correctly
@patch('praw.Reddit', autospec=True)
def test_event_handler(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    test_experiment_name = EXPERIMENT_NAME

    # must create callee_controller instance first, to ensure that Experiment db record exists for it
    # when calling controllers from app/controller.py, you won't write this line. instead,
    # you need to just run initialize_sticky_comment_experiment(experiment_name), 
    # in order to store the Experiment db record. don't run conduct_sticky_comment_experiment,
    # unless the experiment needs a scheduled task that isn't being run in a callback (which is already scheduled)
    callee_controller = StickyCommentExperimentTestController(test_experiment_name, db_session, r, log)

    caller_controller = SomeTestController(db_session, r, log)

    data = caller_controller.test_set_data()
    assert(data == DATA_TEXT)
    assert(caller_controller.num_callbacks_run == 3)