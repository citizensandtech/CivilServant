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
import glob, datetime, time, pytz, math
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
        experiment_config = yaml.full_load(f)['test']

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
        experiment_config = yaml.full_load(f)['test']

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
        experiment_config = yaml.full_load(f)['test']
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
        experiment_config = yaml.full_load(f)['test']
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

def setup_comment_monitoring(r, yesterday_posts, today_posts):
    ####################
    ## SET UP EXPERIMENT
    subreddit_posts = []
    with open(os.path.join(BASE_DIR,"tests", "fixture_data", "subreddit_posts_0" + ".json"), "r") as f:
        subreddit_posts = [z['data'] for z in json.loads(f.read())['data']['children']]

    experiment_name = "stylesheet_experiment_test"
    with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
        experiment_config = yaml.full_load(f)['test']
    controller = StylesheetExperimentController(experiment_name, db_session, r, log)

    today = datetime.datetime.utcnow()
    
    ## add posts created yesterday
    for i in list(range(0,yesterday_posts)):
        post_fixture = subreddit_posts[i]
        post = Post(id = post_fixture['id'],
                    created_at = today - datetime.timedelta(days=1),
                    subreddit_id = controller.subreddit_id,
                    post_data = json.dumps(post_fixture))
        db_session.add(post)
    db_session.commit()
    assert db_session.query(Post).count() == yesterday_posts

    ## add posts created today
    today_post_list = []
    for i in list(range(yesterday_posts,yesterday_posts + today_posts)):
        post_fixture = subreddit_posts[i]
        post = Post(id = post_fixture['id'],
                    created_at = today,
                    subreddit_id = controller.subreddit_id,
                    post_data = json.dumps(post_fixture))
        db_session.add(post)
        today_post_list.append(post)
    db_session.commit()
    assert db_session.query(Post).count() == yesterday_posts + today_posts

    # add experiment_action for the current experiment, at 12:01AM today
    cond = list(controller.experiment_settings['conditions'].keys())[0]
    arm = list(controller.experiment_settings['conditions'][cond].keys())[0]
    action = ExperimentAction(
        created_at = datetime.datetime(year=today.year, month = today.month, day=today.day, hour = 0, minute=1, second=1),
        experiment_id = controller.experiment.id,
        action="Intervention",
        action_object_type=ThingType.STYLESHEET.value,
        action_object_id = None,
        metadata_json = json.dumps({"condition":cond, "arm":arm}))
    db_session.add(action)
    db_session.commit()
    assert db_session.query(ExperimentAction).count() == 1

    # add 5+ toplevel comments to the first half of today_posts
    comment_fixtures = []
    filename = sorted(glob.glob("{script_dir}/fixture_data/comments*".format(script_dir=TEST_DIR)))[0]
    f = open(filename, "r")
    comment_fixtures = json.loads(f.read())
    f.close()

    comment_counter = 0 

    ## add a full quota of comments to the first half of today's posts
    for i in range(0,math.floor(today_posts/2)):
        post = today_post_list[i]
        timestamp = post.created_at + datetime.timedelta(seconds=comment_counter/(i+1))

        for c in range(0, controller.experiment_settings['first_n_comments']):
            comment_dict = comment_fixtures[comment_counter]
            comment_counter += 1
            comment_dict['link_id'] = post.id
            comment_dict['parent_id'] = post.id #make all of them toplevel here

            comment = Comment(
                id = comment_dict['id'],
                created_at = timestamp,
                created_utc = timestamp, #(timestamp - datetime.datetime(1970,1,1)).total_seconds(),
                subreddit_id = controller.subreddit_id,
                post_id = post.id,
                user_id = comment_dict['author'], # in the fixtures, this is the username, weirdly
                comment_data = json.dumps(comment_dict)) # will be inconsistent with main fields here
            db_session.add(comment)

    ## add a partial quota of comments to another 1/4 of today's posts
    num_comments_incomplete = 2
    assert num_comments_incomplete < controller.experiment_settings['first_n_comments']

    for i in range(math.floor(today_posts/2), math.floor(today_posts/2) + math.floor(today_posts/4)):
        post = today_post_list[i]
        timestamp = post.created_at + datetime.timedelta(seconds=comment_counter/(i+1))
         
        for c in range(0, 2):
            comment_dict = comment_fixtures[comment_counter]
            comment_counter += 1

            comment_dict['link_id'] = post.id
            comment_dict['parent_id'] = post.id #make all of them toplevel here

            comment = Comment(
                id = comment_dict['id'],
                created_at = timestamp,
                created_utc = timestamp, #(timestamp - datetime.datetime(1970,1,1)).total_seconds(),
                subreddit_id = controller.subreddit_id,
                post_id = post.id,
                user_id = comment_dict['author'], # in the fixtures, this is the username, weirdly
                comment_data = json.dumps(comment_dict)) # will be inconsistent with main fields here
            db_session.add(comment)

    db_session.commit()
    assert db_session.query(Comment).count() == comment_counter
    return controller, today_post_list, comment_counter

@patch('praw.Reddit', autospec=True)
def test_post_snapshotting(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    yesterday_posts = 10
    today_posts = 20

    controller, today_post_list, comment_counter = setup_comment_monitoring(r, yesterday_posts, today_posts)

    posts = controller.identify_posts_that_need_snapshotting()
    assert len(posts) == today_posts

    assert db_session.query(Post).outerjoin(
           ExperimentThing, Post.id == ExperimentThing.id).filter(
           ExperimentThing.id==None,
           Post.id.in_([x.id for x in posts])).count() == 0

    assert db_session.query(Post).outerjoin(
           ExperimentThing, Post.id == ExperimentThing.id).filter(
           Post.id.in_([x.id for x in posts])).count() == len(posts)

    # now confirm that it doesn't add more ExperimentThings if we run it a second time
    posts = controller.identify_posts_that_need_snapshotting()
    assert len(posts) == today_posts

    assert db_session.query(Post).outerjoin(
           ExperimentThing, Post.id == ExperimentThing.id).filter(
           ExperimentThing.id==None,
           Post.id.in_([x.id for x in posts])).count() == 0

    assert db_session.query(Post).outerjoin(
           ExperimentThing, Post.id == ExperimentThing.id).filter(
           Post.id.in_([x.id for x in posts])).count() == len(posts)

@patch('praw.Reddit', autospec=True)
def test_observe_comment_snapshots(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    yesterday_posts = 10
    today_posts = 20

    # SET UP TEST BY PROPAGATING POSTS AND COMMENTS
    controller, today_post_list, comment_counter = setup_comment_monitoring(r, yesterday_posts, today_posts)
    posts = controller.identify_posts_that_need_snapshotting()
    assert len(posts) == today_posts
    comments = controller.sample_comments(posts)

    ## EXPIRE SOME OF THE COMMENTS
    current_time = datetime.datetime.utcnow()
    expired_time = (current_time - datetime.timedelta(seconds=controller.experiment_settings['intervention_window_seconds'] + 10))
    for i in range(0, math.floor(len(comments)/2)):
        comments[i].created_utc = expired_time
    db_session.commit()

    ## LOAD COMMENT FIXTURES
    comment_fixtures = []
    filename = "{script_dir}/fixture_data/comments_0.json".format(script_dir=TEST_DIR)
    f = open(filename, "r")
    comment_fixtures = json.loads(f.read())
    f.close()
    
    #MOCK RETURN VALUE OF GET_INFO
    r.get_info.return_value = [json2obj(json.dumps(x)) for x in comment_fixtures[0:len(comments) - math.floor(len(comments)/2)]]

    controller.observe_comment_snapshots(comments)
    assert db_session.query(ExperimentThingSnapshot).count() == len(comments) - math.floor(len(comments)/2)

@patch('praw.Reddit', autospec=True)
def test_sample_comments(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    yesterday_posts = 10
    today_posts = 20

    controller, today_post_list, comment_counter = setup_comment_monitoring(r, yesterday_posts, today_posts)
    posts = controller.identify_posts_that_need_snapshotting()
    assert len(posts) == today_posts
    
    comments = controller.sample_comments(posts)
    # quantities defined in setup_comment_monitoring
    assert len(comments) == math.floor(today_posts/2) * controller.experiment_settings['first_n_comments'] + math.floor(today_posts/4)*2
    assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == len(comments)
    orig_comments_length = len(comments)

    ## NOW ENSURE THAT IT DOESN'T ATTEMPT TO RE-ADD NEW COMMENTS
    comments = controller.sample_comments(posts)
    #import pdb;pdb.set_trace()
    assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == orig_comments_length
    for index in [x.post_id for x in comments]:
        assert index in [x.id for x in posts]

    ### NOW ADD SOME MORE COMMENTS AND CONFIRM THAT THESE NEW COMMENTS ARE ADDED APPROPRIATELY
    ### Use index 1 since we use index 0 in the fixture setup
    comment_fixtures = []
    filename = "{script_dir}/fixture_data/comments_0.json".format(script_dir=TEST_DIR)
    f = open(filename, "r")
    comment_fixtures = json.loads(f.read())
    f.close()

    ## FIRST: ADD NEW COMMENTS TO POSTS THAT HAVE ALREADY MET THEIR QUOTA
    for i in range(0,math.floor(today_posts/2)):
        post = today_post_list[i]
        timestamp = post.created_at + datetime.timedelta(seconds=comment_counter/(i+1))

        for c in range(0, controller.experiment_settings['first_n_comments']):
            comment_dict = comment_fixtures[comment_counter]
            comment_counter += 1
            comment_dict['link_id'] = post.id
            comment_dict['parent_id'] = post.id #make all of them toplevel here

            comment = Comment(
                id = comment_dict['id'],
                created_at = timestamp,
                created_utc = timestamp, #(timestamp - datetime.datetime(1970,1,1)).total_seconds(),
                subreddit_id = controller.subreddit_id,
                post_id = post.id,
                user_id = comment_dict['author'], # in the fixtures, this is the username, weirdly
                comment_data = json.dumps(comment_dict)) # will be inconsistent with main fields here
            db_session.add(comment)

    ### NOW CONFIRM THAT THERE IS ONLY CHANGE AMONG POSTS THAT NEEDED MORE COMMENTS
    comments = controller.sample_comments(posts)
    assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == orig_comments_length
    posts_below_quota = 0
    for index,count in Counter([x.post_id for x in comments]).items():
        assert count <= controller.experiment_settings['first_n_comments']
        if count < controller.experiment_settings['first_n_comments']:
            posts_below_quota += 1
      

    ### NOW ADD COMMENTS TO POSTS UNDER THE QUOTA AND CHECK THAT THE NUMBER HAS INCREASED
    for i in range(math.floor(today_posts/2), math.floor(today_posts/2) + math.floor(today_posts/4)):
        post = today_post_list[i]
        timestamp = post.created_at + datetime.timedelta(seconds=comment_counter/(i+1))

        for c in range(0, 2):
            comment_dict = comment_fixtures[comment_counter]
            comment_counter += 1

            comment_dict['link_id'] = post.id
            comment_dict['parent_id'] = post.id #make all of them toplevel here

            comment = Comment(
                id = comment_dict['id'],
                created_at = timestamp,
                created_utc = timestamp, #(timestamp - datetime.datetime(1970,1,1)).total_seconds(),
                subreddit_id = controller.subreddit_id,
                post_id = post.id,
                user_id = comment_dict['author'], # in the fixtures, this is the username, weirdly
                comment_data = json.dumps(comment_dict)) # will be inconsistent with main fields here
            db_session.add(comment)

    db_session.commit()

    comments = controller.sample_comments(posts)
    assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == orig_comments_length + posts_below_quota*2
    for index,count in Counter([x.post_id for x in comments]).items():
        assert count <= controller.experiment_settings['first_n_comments']
