import pytest
import os, yaml

## SET UP THE DATABASE ENGINE
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] = "test"

from mock import Mock, patch
import simplejson as json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime, time
from app.controllers.sticky_comment_experiment_controller import StickyCommentExperimentController
from utils.common import *
from dateutil import parser
import praw

### LOAD THE CLASSES TO TEST
from app.models import *
import app.cs_logger


db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)

def clear_all_tables():
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()
    db_session.query(Comment).delete()
    db_session.query(Experiment).delete()
    db_session.query(ExperimentThing).delete()
    db_session.query(ExperimentAction).delete()
    db_session.commit()    

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()


@patch('praw.Reddit', autospec=True)
def test_initialize_experiment(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    assert(len(db_session.query(Experiment).all()) == 0)
    StickyCommentExperimentController("sticky_comment_0", db_session, r, log)
    assert(len(db_session.query(Experiment).all()) == 1)
    experiment = db_session.query(Experiment).first()
    assert(experiment.name       == "sticky_comment_0")
    assert(experiment.controller == "StickyCommentExperimentController")
#    assert(experiment.start_time == parser.parse("07/22/2016 00:00:00 UTC"))
#    assert(experiment.end_time   == parser.parse("07/23/2016 23:59:59 UTC"))
    
    settings = json.loads(experiment.settings_json)
    assert(settings['comment_text'] == "This is a sticky comment")
    assert(settings['username']     == "CivilServantBot")
    assert(settings['subreddit']    == "natematias")

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_get_eligible_objects(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value
    experiment_name = "sticky_comment_0"

    with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
        experiment_settings = yaml.load(f.read())['test']

    sub_data = []
    with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
        fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
        for post in fixture:
            json_dump = json.dumps(post)
            postobj = json2obj(json_dump)
            sub_data.append(postobj)

    mock_subreddit.get_new.return_value = sub_data
    mock_subreddit.display_name = experiment_settings['subreddit']
    mock_subreddit.name = experiment_settings['subreddit']
    mock_subreddit.id = experiment_settings['subreddit_id']
    r.get_subreddit.return_value = mock_subreddit
    patch('praw.')

    assert(len(db_session.query(Experiment).all()) == 0)
    scec = StickyCommentExperimentController(experiment_name, db_session, r, log)
    assert(len(db_session.query(Experiment).all()) == 1)
    
    ### TEST THE METHOD FOR FETCHING ELIGIBLE OBJECTS
    ### FIRST TIME AROUND
    assert len(db_session.query(Post).all()) == 0
    
    eligible_objects = scec.get_eligible_objects()
    assert len(eligible_objects) == 100
    assert len(db_session.query(Post).all()) == 100

    ### TEST THE METHOD FOR FETCHING ELIGIBLE OBJECTS
    ### SECOND TIME AROUND, WITH SOME ExperimentThing objects stored
    limit = 50

    for post in db_session.query(Post).all():
        limit -= 1
        experiment_thing = ExperimentThing(
            id = post.id, object_type=int(ThingType.SUBMISSION.value))
        db_session.add(experiment_thing)
        if(limit <= 0):
            break
    db_session.commit()
    eligible_objects = scec.get_eligible_objects()
    assert len(eligible_objects) == 50
    

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_assign_randomized_conditions(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value
    experiment_name = "sticky_comment_0"

    with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
        experiment_settings = yaml.load(f.read())['test']

    sub_data = []
    with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
        fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
        for post in fixture:
            json_dump = json.dumps(post)
            postobj = json2obj(json_dump)
            sub_data.append(postobj)
    mock_subreddit.get_new.return_value = sub_data
    mock_subreddit.display_name = experiment_settings['subreddit']
    mock_subreddit.name = experiment_settings['subreddit']
    mock_subreddit.id = experiment_settings['subreddit_id']
    r.get_subreddit.return_value = mock_subreddit
    patch('praw.')

    scec = StickyCommentExperimentController(experiment_name, db_session, r, log)
    eligible_objects = scec.get_eligible_objects()

    experiment_action_count = db_session.query(ExperimentAction).count()

    assert db_session.query(ExperimentThing).count() == 0
    scec.assign_randomized_conditions(eligible_objects)
    assert db_session.query(ExperimentThing).count() == 100
    assert db_session.query(ExperimentAction).count() == experiment_action_count + 1
    assert db_session.query(ExperimentAction).filter(ExperimentAction.action=="SetRandomSeed").count() == experiment_action_count + 1

    for experiment_thing in db_session.query(ExperimentThing).all():
        assert experiment_thing.id != None
        assert experiment_thing.object_type == ThingType.SUBMISSION.value
        assert experiment_thing.experiment_id == scec.experiment.id
        assert "condition" in json.loads(experiment_thing.metadata_json).keys()
    
    ## make sure that no actions are taken if the list is empty
    experiment_action_count = db_session.query(ExperimentAction).count()
    scec.assign_randomized_conditions([])
    assert db_session.query(ExperimentAction).count() == experiment_action_count


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
@patch('praw.objects.Comment', autospec=True)
def test_make_sticky_post(mock_comment, mock_submission, mock_reddit):
    r = mock_reddit.return_value
    experiment_name = "sticky_comment_0"

    with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
        experiment_settings = yaml.load(f.read())['test']

    with open("{script_dir}/fixture_data/submission_0.json".format(script_dir=TEST_DIR)) as f:
        submission_json = json.loads(f.read())
        ## setting the submission time to be recent enough
        submission = json2obj(json.dumps(submission_json))
        mock_submission.id = submission.id
    
    with open("{script_dir}/fixture_data/submission_0_comments.json".format(script_dir=TEST_DIR)) as f:
        comments = json2obj(f.read())
        mock_submission.comments.return_value = comments

    with open("{script_dir}/fixture_data/submission_0_treatment.json".format(script_dir=TEST_DIR)) as f:
        treatment = json2obj(f.read())
        mock_comment.id = treatment.id
        mock_comment.created_utc = treatment.created_utc
        mock_submission.add_comment.return_value = mock_comment

    with open("{script_dir}/fixture_data/submission_0_treatment_distinguish.json".format(script_dir=TEST_DIR)) as f:
        distinguish = json.loads(f.read())
        mock_comment.distinguish.return_value = distinguish

    patch('praw.')

    scec = StickyCommentExperimentController(experiment_name, db_session, r, log)

    ## First, try to intervene on a submission with an old timestamp
    ## which should return None and take no action
    assert db_session.query(ExperimentAction).count() == 0
    mock_submission.created_utc = int(time.time()) - 1000
    sticky_result = scec.make_sticky_post(mock_submission)
    assert sticky_result is None
    assert db_session.query(ExperimentAction).count() == 0

    ## Now try to intervene on a more recent post
    mock_submission.created_utc = int(time.time())
    assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == 0
    sticky_result = scec.make_sticky_post(mock_submission)
    assert db_session.query(ExperimentAction).count() == 1
    assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == 1
    assert sticky_result is not None

    ## make sure it aborts the call if we try a second time
    sticky_result = scec.make_sticky_post(mock_submission)
    assert db_session.query(ExperimentAction).count() == 1
    assert sticky_result is None


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
@patch('praw.objects.Comment', autospec=True)
def test_make_control_nonaction(mock_comment, mock_submission, mock_reddit):
    r = mock_reddit.return_value
    experiment_name = "sticky_comment_0"

    with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
        experiment_settings = yaml.load(f.read())['test']

    with open("{script_dir}/fixture_data/submission_0.json".format(script_dir=TEST_DIR)) as f:
        submission_json = json.loads(f.read())
        ## setting the submission time to be recent enough
        submission = json2obj(json.dumps(submission_json))
        mock_submission.id = submission.id    

    scec = StickyCommentExperimentController(experiment_name, db_session, r, log)
    mock_submission.created_utc = int(time.time())
    sticky_result = scec.make_control_nonaction(mock_submission)
    assert db_session.query(ExperimentAction).count() == 1
    assert sticky_result is not None

    ## make sure it aborts the call if we try a second time
    sticky_result = scec.make_control_nonaction(mock_submission)
    assert db_session.query(ExperimentAction).count() == 1
    assert sticky_result is None
    
@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
@patch('praw.objects.Comment', autospec=True)
def test_remove_replies_to_sticky_comments(mock_comment,mock_submission,mock_reddit):
    r = mock_reddit.return_value
    experiment_name = "sticky_comment_0"

    with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
        experiment_settings = yaml.load(f.read())['test']

    with open("{script_dir}/fixture_data/submission_0.json".format(script_dir=TEST_DIR)) as f:
        submission_json = json.loads(f.read())
        ## setting the submission time to be recent enough
        submission = json2obj(json.dumps(submission_json))
        mock_submission.id = submission.id

    with open("{script_dir}/fixture_data/submission_0_treatment.json".format(script_dir=TEST_DIR)) as f:
        treatment = json2obj(f.read())
        mock_comment.id = treatment.id
        mock_comment.created_utc = treatment.created_utc
        mock_submission.add_comment.return_value = mock_comment

    with open("{script_dir}/fixture_data/submission_0_treatment_distinguish.json".format(script_dir=TEST_DIR)) as f:
        distinguish = json.loads(f.read())
        mock_comment.distinguish.return_value = distinguish

    with open("{script_dir}/fixture_data/flattened_comment_replies_0.json".format(script_dir=TEST_DIR)) as f:
        flattened_comments = json.loads(f.read())
        mock_comment.replies = json2obj(json.dumps(flattened_comments))
        r.get_info.return_value = mock_comment

    patch('praw.')

    scec = StickyCommentExperimentController(experiment_name, db_session, r, log)

    ## Add the sticky comment
    mock_submission.created_utc = int(time.time())
    sticky_result = scec.make_sticky_post(mock_submission)
    assert db_session.query(ExperimentAction).count() == 1
    assert sticky_result is not None
    #import pdb; pdb.set_trace()

