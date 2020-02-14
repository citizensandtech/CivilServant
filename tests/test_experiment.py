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
from app.controllers.messaging_experiment_controller import *
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
        #"sticky_comment_0": AMAStickyCommentExperimentController,
        #"sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController,
        "messaging_experiment_test": MessagingExperimentController
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
                    'username', 'max_eligibility_age', 'min_eligibility_age', 'newcomer_maximum_age_in_days']:
            if key in controller_instance.required_keys:
                assert(settings[key] == experiment_config[key])

        ### NOW TEST THAT CONDITIONS ARE ADDED
        for condition_name in experiment_config['conditions']:
            with open(os.path.join(BASE_DIR,"config", "experiments", experiment_config['conditions'][condition_name]['randomizations']), "r") as f:
                conditions = []
                for row in csv.DictReader(f):
                    conditions.append(row)
            assert len(settings['conditions'][condition_name]['randomizations']) == len(conditions)
            assert settings['conditions'][condition_name]['next_randomization']  == 0

        clear_all_tables()

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_identify_condition(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController#,
        #"mod_user_test": ModUserExperimentController
    }

    ############

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        sub_data = []
        with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
            fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
            min_age = experiment_settings["min_eligibility_age"]
            for post in fixture:
                json_dump = json.dumps(post)
                postobj = json2obj(json_dump, now=True, offset=-1*min_age)
                sub_data.append(postobj)
        mock_subreddit.get_new.return_value = sub_data

        mock_subreddit.display_name = experiment_settings['subreddit']
        mock_subreddit.name = experiment_settings['subreddit']
        mock_subreddit.id = experiment_settings['subreddit_id']
        r.get_subreddit.return_value = mock_subreddit
        patch('praw.')


        ################################################

        ## TEST THE BASE CASE OF RANDOMIZATION
        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        # "mock" FrontPageController.posts, ModeratorController.mod_actions
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            instance = FrontPageController(db_session, r, log)
            instance.posts = sub_data
        #elif controller_instance.__class__ is ModUserExperimentController:
        #    modaction_pages = []
        #    modaction_data = []
        #    for filename in sorted(glob.glob("{script_dir}/fixture_data/mod_action*".format(script_dir=TEST_DIR))):
        #        with open(filename, "r") as f:
        #            mod_action_list = []
        #            for mod_action in json.loads(f.read()):
        #                mod_action['sr_id36'] = controller_instance.subreddit_id
        #                mod_action_list.append(json2obj(json.dumps(mod_action)))
        #            modaction_pages.append(mod_action_list)
        #    modaction_data = modaction_pages[0]
        #    log.info("************** {0}".format(len(modaction_data)))
        #    #r.get_mod_log.return_value = modaction_data
        #    instance = ModeratorController(experiment_settings['subreddit'], db_session, r, log)
        #    instance.mod_actions = modaction_data

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects(instance)
            eligible_objects = controller_instance.get_eligible_objects(objs)
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()
            eligible_objects = controller_instance.get_eligible_objects(objs)            
        #elif controller_instance.__class__ is ModUserExperimentController:
        #    eligible_objects = controller_instance.get_eligible_users_and_archive_mod_actions(instance)

        condition_list = []
        for obj in eligible_objects:
            condition_list.append(controller_instance.identify_condition(obj))

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            assert Counter(condition_list)['frontpage_post'] == 100
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            assert Counter(condition_list)['nonama'] == 98
            assert Counter(condition_list)['ama'] == 2
        #elif controller_instance.__class__ is ModUserExperimentController:
        #    assert Counter(condition_list)['main'] == 100 ###            

        clear_all_tables()
