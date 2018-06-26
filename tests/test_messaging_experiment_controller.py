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
        assert last_log_line.find("MessagingExperimentController::enroll_new_participants") > -1
    else:
        ## IF THERE'S NO CONCURRENT ROTATING FILE HANDLER
        ## RETURN FALSE
        assert False


#### TEST MessagingExperimentController::identify_newcomers
#### TEST MessagingExperimentController::previously_enrolled
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


#### TEST MessagingExperimentController::get_accounts_needing_interventions
@patch('praw.Reddit', autospec=True)
def test_interventions(mock_reddit):
    r = mock_reddit.return_value

    ## SET UP FIXTURES AND INITIALIZE DATABASE

    accounts_to_test = 4

    account_comments = defaultdict(list)
    comment_fixtures = []
    newcomer_comments = []
    for filename in sorted(glob.glob("{script_dir}/fixture_data/comments*".format(script_dir=TEST_DIR))):
        f = open(filename, "r")
        comment_fixtures += json.loads(f.read())
        f.close()
    
    ## IN THIS CASE, WE ARE GENERATING AUTHOR IDs
    ## LEST A BUG ACCIDENTALLY SEND PEOPLE COMMENTS
    ## WHILE WE ARE UNIT TESTING. reddit has a 20 character limit
    ## so any uuid4 will be an invalid username on reddit
    for comment in comment_fixtures:
        author = uuid.uuid4().hex
        comment['author']  = author
        account_comments[author].append(comment)
    for author, comments in account_comments.items():
        comments = sorted(comments, key = lambda x : x['created_utc'])
        newcomer_comments.append({"author": author, "comment":comments[0]})


    mec = NewcomerMessagingExperimentController("newcomer_messaging_experiment_test", db_session, r, log)
    mec.assign_randomized_conditions(newcomer_comments[0:accounts_to_test])

    ## TEST the result from get accounts needing intervention
    accounts_needing_intervention = mec.get_accounts_needing_interventions()
    assert len(accounts_needing_intervention) == len(newcomer_comments[0:accounts_to_test])
    newcomer_authors = [x['author'] for x in newcomer_comments]
    for account in accounts_needing_intervention:
        assert account.thing_id in newcomer_authors

    ## TEST the formatting of messages
    # first case: where the arm is arm_1 as specified in the randomizations csv
    arm_1_experiment_thing = [x for x in accounts_needing_intervention if json.loads(x.metadata_json)['arm']=="arm_1"][0]

    message_output = mec.format_message(arm_1_experiment_thing)
    assert message_output['message'].find("Hi {0}!".format(arm_1_experiment_thing.thing_id)) > -1
    # second case: where the arm is null because it's the control group
    # in that case, the message output should be None

    arm_0_experiment_thing = [x for x in accounts_needing_intervention if json.loads(x.metadata_json)['arm']=="arm_0"][0]
    message_output = mec.format_message(arm_0_experiment_thing) 
    assert message_output is None

    ## TEST the result from sending messages
    m = Mock()
    message_return_vals = []

    ## SET UP accounts_to_test return values from message sending
    ## the final account in the set will be an invalid username error
    #for i in range(accounts_to_test-1):
    #    message_return_vals.append({"errors":[]})
    message_return_vals.append({"errors":[]})
    message_return_vals.append({"errors":[{"username":newcomer_authors[accounts_to_test -2],
                                "error": "nondescript error"}]})    
    message_return_vals.append(
        {"errors":[{"username":newcomer_authors[accounts_to_test-1], 
        "error":"invalid username"}]})

    m.side_effect = message_return_vals
    r.send_message = m
    patch('praw.')

    experiment_things = accounts_needing_intervention[0:accounts_to_test]
    
    message_results = mec.send_messages(experiment_things)
    
    ## assertions for ExperimentAction objects
    experiment_actions = db_session.query(ExperimentAction).all()
    assert len(experiment_actions) == 3
    ea = json.loads(experiment_actions[0].metadata_json)
    assert(ea['survey_status']=="TBD")
    assert(ea['message_status']=="sent")
    ea = json.loads(experiment_actions[1].metadata_json)
    assert(ea['survey_status']=="TBD")
    assert(ea['message_status']=="sent")
    ea = json.loads(experiment_actions[2].metadata_json)
    assert(ea['survey_status']=="nonexistent")
    assert(ea['message_status']=="nonexistent")

    ## assertions for ExperimentThing objects
    outcome_bundles = [{"query_index": "Intervention Complete",
                    "message_status": "sent",
                    "survey_status": "TBD",
                    "observed_count":0},
                    {"query_index":"Intervention TBD",
                     "message_status":"TBD",
                     "survey_status":"TBD",
                     "observed_count":0},
                    {"query_index":"Intervention Impossible",
                     "message_status":"nonexistent",
                     "survey_status":"nonexistent",
                     "observed_count":0}
                     ]

    for experiment_thing in experiment_things:
        mj = json.loads(experiment_thing.metadata_json)
        for outcome in outcome_bundles:
            if(experiment_thing.query_index == outcome['query_index'] and 
                mj['message_status'] == outcome['message_status'] and
                mj['survey_status'] == outcome['survey_status']
              ):
              outcome['observed_count'] += 1

    assert outcome_bundles[0]['observed_count'] == 2
    assert outcome_bundles[1]['observed_count'] == 1
    assert outcome_bundles[2]['observed_count'] == 1
