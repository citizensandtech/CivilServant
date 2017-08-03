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
from app.controllers.sticky_comment_experiment_controller import *
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
    patch('praw.')

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController,
        "mod_user_test": ModUserExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
            experiment_config = yaml.load(f)['test']

        assert(len(db_session.query(Experiment).all()) == 0)

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        assert(len(db_session.query(Experiment).all()) == 1)
        experiment = db_session.query(Experiment).first()
        assert(experiment.name       == experiment_name)
        assert(experiment.controller == experiment_config['controller'])
        assert(pytz.timezone("UTC").localize(experiment.start_time) == parser.parse(experiment_config['start_time']))
        assert(pytz.timezone("UTC").localize(experiment.end_time)   == parser.parse(experiment_config['end_time']))
        
        settings = json.loads(experiment.settings_json)

        for key in ['subreddit', 'subreddit_id', 'shadow_subreddit', 'shadow_subreddit_id', 
                    'username', 'max_eligibility_age', 'min_eligibility_age']:
            if key in controller_instance.required_keys:
                assert(settings[key] == experiment_config[key])

        ### NOW TEST THAT AMA OBJECTS ARE ADDED
        for condition_name in experiment_config['conditions']:
            with open(os.path.join(BASE_DIR,"config", "experiments", experiment_config['conditions'][condition_name]['randomizations']), "r") as f:
                conditions = []
                for row in csv.DictReader(f):
                    conditions.append(row)

            with open(os.path.join(BASE_DIR,"config", "experiments", experiment_config['conditions'][condition_name]['randomizations']), "r") as f:
                nonconditions = []
                for row in csv.DictReader(f):
                    nonconditions.append(row)

            assert len(settings['conditions'][condition_name]['randomizations']) == len(conditions)
            assert settings['conditions'][condition_name]['next_randomization']     == 0

        clear_all_tables()