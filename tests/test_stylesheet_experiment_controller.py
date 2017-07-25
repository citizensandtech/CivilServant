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
from app.controllers.stylesheet_experiment_controller import *

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

    experiment_name = "stylesheet_experiment_test"

    with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
        experiment_config = yaml.load(f)['test']

    assert len(db_session.query(Experiment).all()) == 0
    controller = StylesheetExperimentController(experiment_name, db_session, r, log)
    assert len(db_session.query(Experiment).all()) == 1
    experiment = controller.experiment
    assert experiment.name == experiment_name

    assert(experiment.controller == experiment_config['controller'])

    settings = json.loads(experiment.settings_json)
    for k in ['username', 'subreddit', 'subreddit_id', 'start_time', 'end_time', 'controller']:
        assert settings[k] == experiment_config[k]

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
        assert settings['conditions'][condition_name]['next_randomization']  == 0

@patch('praw.Reddit', autospec=True)    
def test_determine_intervention_eligible(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    experiment_name = "stylesheet_experiment_test"
    with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
        experiment_config = yaml.load(f)['test']

    assert len(db_session.query(Experiment).all()) == 0
    controller = StylesheetExperimentController(experiment_name, db_session, r, log)

    ## in the case with no interventions, confirm eligibility
    assert controller.determine_intervention_eligible() == True

    ## now create an action and confirm ineligibility outside the interval
    experiment_action = ExperimentAction(
        experiment_id = controller.experiment.id,
        praw_key_id = "TEST",
        action = "Intervention:{0}.{1}".format("TEST","TEST"),
        action_object_type = ThingType.STYLESHEET.value,
        action_object_id = None,
        metadata_json  = json.dumps({"arm":"TEST", "condition":"TEST"})
    )
    db_session.add(experiment_action)
    db_session.commit()

    assert controller.determine_intervention_eligible() == False
    
    ## now change the action and confirm eligibility within the interval
    experiment_action.created_at = experiment_action.created_at - datetime.timedelta(seconds=controller.experiment_settings['intervention_interval_seconds'])
    db_session.commit()
    assert controller.determine_intervention_eligible() == True

    ## now change the end date of the experiment and confirm ineligibility
    controller.experiment_settings['end_time'] = str((datetime.datetime.utcnow() - datetime.timedelta(days=1)).replace(tzinfo=pytz.utc))
    #controller.experiment.settings = json.dumps(controller.experiment_settings)
    #db_session.commit()
    assert controller.determine_intervention_eligible() == False
    

@patch('praw.Reddit', autospec=True)    
def test_select_condition(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    experiment_name = "stylesheet_experiment_test"
    with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
        experiment_config = yaml.load(f)['test']
    controller = StylesheetExperimentController(experiment_name, db_session, r, log)
    
    assert controller.select_condition(current_time = parser.parse("07/21/2017 00:00:00")) == "special"
    assert controller.select_condition(current_time = parser.parse("07/20/2017 00:00:00")) == "normal"


@patch('praw.Reddit', autospec=True)
def test_set_stylesheet(mock_reddit):
    r = mock_reddit.return_value
    with open(os.path.join(BASE_DIR,"tests", "fixture_data", "stylesheet_0" + ".json"), "r") as f:
        stylesheet = json.loads(f.read())
    r.get_stylesheet.return_value = stylesheet
    r.set_stylesheet.return_value = {"errors":[]}
    patch('praw.')

    experiment_name = "stylesheet_experiment_test"
    with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
        experiment_config = yaml.load(f)['test']
    controller = StylesheetExperimentController(experiment_name, db_session, r, log)

    for condition in ['special', 'normal']:
        for arm in ["arm_0", "arm_1"]:
            assert (controller.experiment_settings['conditions'][condition]['arms'][arm] in stylesheet['stylesheet'].split("\n"))!=True

    for condition in ['special', 'normal']:
        for arm in ["arm_0", "arm_1"]:
            line_length = len(stylesheet['stylesheet'].split("\n"))
            result_lines = controller.set_stylesheet(condition, arm).split("\n")
            assert controller.experiment_settings['conditions'][condition]['arms'][arm] in result_lines
            assert len(result_lines) == line_length + 3