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
from app.controllers.messaging_experiment_controller import *
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
    mec = MessagingExperimentController("messaging_experiment_test", db_session, r, log)
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
    log_filename = log.handlers[0].baseFilename
    last_log_line = None
    with open(log_filename, "r") as f:
        for line in f:
            last_log_line = line

    assert last_log_line.find("MessagingExperimentController::enroll_new_participants") > -1


#### TEST MessagingExperimentController::previously_enrolled
@patch('praw.Reddit', autospec=True)
def test_previously_enrolled(mock_reddit):
    assert True


@patch('praw.Reddit', autospec=True)
def test_newcomer_messaging_eligibility(mock_reddit):
    random.seed(26062490)
    r = mock_reddit.return_value
    mec = NewcomerMessagingExperimentController("newcomer_messaging_experiment_test", db_session, r, log)
    subreddit_name = mec.experiment_settings['subreddit']
    subreddit_id = mec.experiment_settings['subreddit_id']

    current_date = datetime.datetime.utcnow()
    newcomer_period_start = current_date - datetime.timedelta(
            days = mec.experiment_settings['newcomer_period_interval_days'])

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

    ## ADD COMMENTS TO THE DATABASE:
    ## Set half of the comment accounts
    ## to have comment datetimes from before the newcomer cutoff
    ## we randomly shuffle the accounts to ensure
    ## roughly equal distribution of before/after comment times
    # across the comment fixture dataset

    unique_accounts = list(set([comment['author'] for comment in comment_fixtures[0]]))
    random.shuffle(unique_accounts)
    midpoint_index = math.floor(len(unique_accounts)/2)
    before_midpoint_accounts = unique_accounts[0:midpoint_index]
    after_midpoint_accounts = unique_accounts[midpoint_index:]

    ## now reserve half of the after midpoint comments to be newcomer account comments
    newcomer_accounts = copy.copy(after_midpoint_accounts)
    random.shuffle(newcomer_accounts)
    newcomer_accounts = newcomer_accounts[0:int(len(newcomer_accounts)/2)]
    newcomer_comments = []

    before_period_start = newcomer_period_start - datetime.timedelta(days = 1)
    after_period_start = newcomer_period_start + datetime.timedelta(days = 1)
    for comment in comment_fixtures[0]:
        if comment['author'] in before_midpoint_accounts:
            comment['created_utc'] = comment['created'] = time.mktime(before_period_start.timetuple())
        else:
            comment['created_utc'] = comment['created'] = time.mktime(after_period_start.timetuple())
        if comment['author'] in newcomer_accounts:
            newcomer_comments.append(comment)

    db_comments = []
    saved_comments = []
    for comment in comment_fixtures[0]:
        if comment['author'] in newcomer_accounts:
            continue
        saved_comments.append(comment)
        db_comments.append({
            "id": comment['id'],
            "subreddit_id": subreddit_id,
            "created_utc": datetime.datetime.utcfromtimestamp(comment['created_utc']),
            "post_id": comment['link_id'].replace("t3_" ,""),
            "user_id": comment['author'],
            "comment_data": json.dumps(comment)
        })
    
    db_session.insert_retryable(Comment, db_comments)
    assert db_session.query(Comment).count() == len(comment_fixtures[0]) - len(newcomer_comments)

    #### TEST NewcomerMessagingExperimentController::identify_newcomers
    ## First, create a dataset of non-newcomer "new" comments
    non_newcomer_new_comments = copy.copy(saved_comments)
    random.shuffle(non_newcomer_new_comments)
    non_newcomer_new_comments = non_newcomer_new_comments[0:100]
    
    # Then give these "new" comments unique IDs.
    # we know these are unique because comment IDs in reddit
    # are in a different base than base 10
    i = 0
    for comment in non_newcomer_new_comments:
        comment['id'] = str(i)
    non_newcomer_accounts = list(set([x['author'] for x in non_newcomer_new_comments]))

    identified_newcomers = mec.identify_newcomers(newcomer_comments + non_newcomer_new_comments)
    identified_newcomer_names = [x['author'] for x in identified_newcomers]

    for author in identified_newcomer_names:
        assert author in newcomer_accounts
        assert author not in non_newcomer_accounts
    
    ### TEST NewcomerMessagingExperimentController:: assign_randomized_conditions
    ## This method should create an ExperimentThing for each newcomer that
    ## is eligible for participation in the study
    ## In the newcomer experiment, eligibility is determined by whether
    ## a newcomer has previously been assigned to a condition or not

    ## NOTE: In the removal message experiment (TODO), eligibility is determined
    ## by whether an account has previously been assigned to a condition or not
    ## within that thread
    mec.assign_randomized_conditions(identified_newcomers)
    
    ## note that the thing_id is the username not the reddit ID
    et_newcomers = db_session.query(ExperimentThing).filter(and_(
        ExperimentThing.thing_id.in_(identified_newcomer_names),
        ExperimentThing.experiment_id == mec.experiment.id,
        ExperimentThing.query_index == "Intervention TBD"
    )).all()

    assert len(et_newcomers) == len(identified_newcomers)
    for newcomer_thing in et_newcomers:
        metadata = json.loads(newcomer_thing.metadata_json)
        assert "arm" in metadata.keys()
        assert "condition" in metadata.keys()
        assert "randomization" in metadata.keys()
        assert "submission_id" in metadata.keys()
        assert metadata['message_status'] == "TBD"
        assert metadata['survey_status'] == "TBD"

    ## NOW test previously_enrolled
    previously_enrolled_accounts = mec.previously_enrolled(identified_newcomer_names + ["natematias"])
    for account in previously_enrolled_accounts:
        assert account in identified_newcomer_names
    assert "natematias" not in previously_enrolled_accounts

    ## NOW CONFIRM THAT MORE newcomer_things aren't assigned
    ## if we identify the same set of newcomers
    
    randomization_number = mec.experiment_settings['conditions']['main']['randomizations']
    mec.assign_randomized_conditions(identified_newcomers)
    assert mec.experiment_settings['conditions']['main']['randomizations'] == randomization_number

    et_newcomers = db_session.query(ExperimentThing).all()

    assert len(et_newcomers) == len(identified_newcomers)