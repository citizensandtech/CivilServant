import pytest
import os
from mock import Mock, patch
import simplejson as json
import sqlalchemy
from sqlalchemy import create_engine, func, or_, and_
from sqlalchemy.orm import sessionmaker
import glob, datetime
import app.controllers.twitter_observational_analysis_controller
from utils.common import PageType, DbEngine, json2obj, TwitterUserState
import requests
import twitter

import utils
from utils.common import CS_JobState, NOT_FOUND_TWITTER_USER_STR, generate_not_found_twitter_user_id
from utils.common import TwitterUserState as TUS 

### LOAD THE CLASSES TO TEST
from app.models import Base, FrontPage, SubredditPage, Subreddit, Post, ModAction, Comment, User, LumenNotice, LumenNoticeToTwitterUser, TwitterUser, TwitterStatus, TwitterUserSnapshot
import app.cs_logger

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] ="test"

db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)

def clear_all_tables():
    db_session.rollback()
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()  
    db_session.query(ModAction).delete()    
    db_session.query(Comment).delete()      
    db_session.query(LumenNotice).delete()    
    db_session.query(LumenNoticeToTwitterUser).delete()
    db_session.query(TwitterUser).delete()
    db_session.query(TwitterUserSnapshot).delete()    
    db_session.query(TwitterStatus).delete()                  
    db_session.commit()

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()

START_DATE = datetime.datetime.strptime("2017-01-01", "%Y-%m-%d")
END_DATE = datetime.datetime.strptime("2017-01-02", "%Y-%m-%d")    
MIN_OBSERVED_DAYS = 7

user_e_id = generate_not_found_twitter_user_id("username_c")
CONFIG = [
    {       # user a: always found; enough snapshots
        "user_id": "user_id_a",
        "not_found_id": None,
        "screen_name": "username_a",
        "lumen_notices_count": 2,
        "user_states": [(0, TUS.FOUND), (1, TUS.FOUND), (2, TUS.FOUND), (3, TUS.FOUND), 
                        (4, TUS.FOUND), (5, TUS.FOUND), (6, TUS.FOUND), (7, TUS.FOUND)],
        "tweet_counts": [(-8, 2), (-5, 2), (-4, 2), (-2, 2), (-1, 2), (0, 1), 
                         (1, 3), (2, 2), (3, 2), (5, 2), (7,2)],
        "is_eligible": True
    }, {    # user b: suspended, found, not_found; enough snapshots
        "user_id": "user_id_b",
        "not_found_id": generate_not_found_twitter_user_id("username_b"),
        "screen_name": "username_b",
        "lumen_notices_count": 1,
        "user_states": [(0, TUS.SUSPENDED), (1, TUS.SUSPENDED), (2, TUS.FOUND), (3, TUS.FOUND), 
                        (4, TUS.FOUND), (5, TUS.FOUND), (6, TUS.FOUND), (7, TUS.FOUND)],
        "tweet_counts": [(-10, 2), (3, 2)],
        "is_eligible": True
    }, {    # user c: protected, found; enough snapshots
        "user_id": "user_id_c",
        "not_found_id": generate_not_found_twitter_user_id("username_c"),
        "screen_name": "username_c",
        "lumen_notices_count": 1,
        "user_states": [(0, TUS.FOUND), (1, TUS.FOUND), (2, TUS.FOUND), (3, TUS.FOUND), 
                        (4, TUS.FOUND), (5, TUS.FOUND), (6, TUS.PROTECTED), (7, TUS.FOUND)],
        "tweet_counts": [(-8, 2), (-7, 2), (-5, 2), (-4, 2), (-2, 2), (-1, 2), (0, 1), 
                         (1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (7,3)],
        "is_eligible": True
    }, {    # user d: suspended; not enough snapshots - will be pruned
        "user_id": "user_id_d",
        "not_found_id": generate_not_found_twitter_user_id("username_d"),
        "screen_name": "username_d",
        "lumen_notices_count": 1,
        "user_states": [(0, TUS.FOUND), (1, TUS.FOUND), (2, TUS.FOUND), (3, TUS.FOUND), 
                        (4, TUS.FOUND), (5, TUS.SUSPENDED)],
        "tweet_counts": [(-8, 2), (0, 2), 
                         (1, 2), (2, 2), (5,2)], # # we cut out users that don't have one snapshot per day
        "is_eligible": False
    }, {    # user e: not found id. will be disqualified bc can't find user states, tweets
        "user_id": user_e_id,
        "not_found_id": user_e_id,
        "screen_name": "username_e",
        "lumen_notices_count": 2,
        "user_states": [(0, TUS.NOT_FOUND)], # can't find
        "tweet_counts": [], # can't find
        "is_eligible": False    
    }
]
ELIGIBLE_COUNT = len([user for user in CONFIG if user["is_eligible"]]) # of users
NOT_ELIGIBLE_COUNT = len([user for user in CONFIG if not user["is_eligible"]]) # of users

#questions - 
# decide whether to cut out users that don't have one snapshot per day
# enough day nums back only depends on whether or not you have any tweets at all from before? is that a good way?

def initialize_default_controller():
    output_dir = "/" # not using
    to = app.controllers.twitter_observational_analysis_controller.TwitterObservationalAnalysisController(
        START_DATE, END_DATE, MIN_OBSERVED_DAYS, output_dir, db_session, log)
    return to


def load_test_db_records(config):
    load_lumen_notices(eligible_count=ELIGIBLE_COUNT, not_eligible_notices_count=3)
    load_lumen_notice_to_twitter_user(config, eligible_count=ELIGIBLE_COUNT)
    load_twitter_users_and_snapshots_and_tweets(config)

def load_lumen_notices(eligible_count=ELIGIBLE_COUNT, not_eligible_notices_count=3):
    with open("{script_dir}/fixture_data/anon_lumen_notices_0.json".format(script_dir=TEST_DIR)) as f:
        notices_json = json.loads(f.read())["notices"]

    for i, notice in enumerate(notices_json[:eligible_count+not_eligible_notices_count]):
        nid = notice["id"]
        date_received = datetime.datetime.strptime(notice["date_received"], '%Y-%m-%dT%H:%M:%S.000Z') # expect string like "2017-04-15T22:28:26.000Z"
        sender = (notice["sender_name"].encode("utf-8", "replace") if notice["sender_name"] else "")
        principal = (notice["principal_name"].encode("utf-8", "replace") if notice["principal_name"] else "")
        recipient = (notice["recipient_name"].encode("utf-8", "replace") if notice["recipient_name"] else "")
        if i < eligible_count:
            date = START_DATE + datetime.timedelta(hours=i) # just make them all on start_date
        else:
            date = END_DATE + datetime.timedelta(hours=i) # out of date range    

        notice_record = LumenNotice(
            id = nid,
            record_created_at = datetime.datetime.utcnow(),
            date_received = date,
            sender = sender,
            principal = principal,
            recipient = recipient,
            notice_data = json.dumps(notice).encode("utf-8", "replace"),
            CS_parsed_usernames = CS_JobState.NOT_PROCESSED.value)
        db_session.add(notice_record)

    db_session.commit()
    assert db_session.query(LumenNotice).count() == eligible_count+not_eligible_notices_count


# user A: found, 1 notice
# user B: 2 notices
# user C: username, id=NOT_FOUND_TWITTER_USER_STR
def load_lumen_notice_to_twitter_user(config, eligible_count=ELIGIBLE_COUNT):
    notices = db_session.query(LumenNotice).filter(
        and_(LumenNotice.date_received >= START_DATE,
            LumenNotice.date_received <= END_DATE)).all()
    assert len(notices) == eligible_count

    now = datetime.datetime.utcnow()

    for i, user in enumerate(config):
        for j in range(user["lumen_notices_count"]):
            notice_id = notices[(i+j) % len(notices)].id

            notice_user_record = LumenNoticeToTwitterUser(
                    record_created_at = now,
                    notice_id = notice_id,
                    twitter_username = user["screen_name"],
                    twitter_user_id = user["user_id"],
                    CS_account_archived = CS_JobState.NOT_PROCESSED.value
                )
            db_session.add(notice_user_record)
    db_session.commit()

    assert db_session.query(LumenNoticeToTwitterUser).count() == sum([user["lumen_notices_count"] for user in config])

def load_twitter_users_and_snapshots_and_tweets(config):
    with open("{script_dir}/fixture_data/anon_twitter_users.json".format(script_dir=TEST_DIR)) as f:
        users_json = json.loads(f.read())
    user_i = 0

    with open("{script_dir}/fixture_data/anon_twitter_tweets.json".format(script_dir=TEST_DIR)) as f:
        tweets_json = json.loads(f.read())
    tweet_i = 0

    now = datetime.datetime.utcnow()
    for i, user in enumerate(config):

        has_been_found = False
        has_been_not_found = False
        stored_user = False        
        for (day, state) in user["user_states"]:
            if state==TUS.FOUND:
                has_been_found = True
            if state!=TUS.FOUND:
                has_been_not_found = True

            if not stored_user:
                #user_data = users_json[i] # currently not modifying this fixture data since our tests don't rely on it
                user_record = TwitterUser(
                    id = user["user_id"] if has_been_found else user["not_found_id"],
                    not_found_id = user["not_found_id"] if has_been_not_found else None,
                    screen_name = user["screen_name"],
                    created_at = None, # not mocking right now
                    record_created_at = now,
                    lang = "en",
                    user_state = [state for (day, state) in user["user_states"] if day==0][0].value,                
                    CS_oldest_tweets_archived = CS_JobState.NOT_PROCESSED.value)
                db_session.add(user_record)                
                stored_user = True

            user_snapshot_record = TwitterUserSnapshot(
                twitter_user_id = user["user_id"] if has_been_found else user["not_found_id"],
                twitter_not_found_id = user["not_found_id"] if has_been_not_found else None,
                record_created_at = START_DATE + datetime.timedelta(days=day, minutes=60),
                user_state = state.value,
                user_json = json.dumps(users_json[user_i])
            )
            user_i+=1
            db_session.add(user_snapshot_record)

        for (day, count) in user["tweet_counts"]:
            for tweet_j in range(count):
                tweet = tweets_json[tweet_i]

                status_record = TwitterStatus(
                    id = tweet["id"], #########
                    user_id = user["user_id"],
                    record_created_at = datetime.datetime.utcnow(),
                    created_at = START_DATE + datetime.timedelta(days=day, minutes=30 + tweet_j),
                    status_data = json.dumps(tweet))
                db_session.add(status_record)
                tweet_i+=1

    db_session.commit()

    assert db_session.query(TwitterUser).count() == ELIGIBLE_COUNT+NOT_ELIGIBLE_COUNT
    assert db_session.query(TwitterUserSnapshot).count() == sum([len(user["user_states"]) for user in config])
    assert db_session.query(TwitterStatus).count() == sum([sum([count for (day, count) in user["tweet_counts"]]) for user in config])


################################################################################
################################################################################


# unless otherwise stated, assume that subsequent method calls 
# in create_datasets() depend on effects of previous methods

# in create_datasets(), sets self.twitter_users_to_notice_dates
# self.twitter_users_to_notice_dates = {user_id: [notice_date_received_day]}
def test_get_users_to_notice_dates():
    # load LumenNotice, some within date range, some not within
    # load LumenNoticeToTwitterUser, some with found user_id, some with not_found_id
    load_test_db_records(CONFIG)

    # pre
    to = initialize_default_controller()


    # test
    twitter_users_to_notice_dates = to.get_users_to_notice_dates()

    # asserts
    print(twitter_users_to_notice_dates)
    assert len(twitter_users_to_notice_dates) == ELIGIBLE_COUNT+NOT_ELIGIBLE_COUNT #
    for user in CONFIG:
        assert len(twitter_users_to_notice_dates[user["user_id"]]) == user["lumen_notices_count"]    


# in create_datasets(), sets self.twitter_users_to_snapshots
# modifies self.user_ids_to_not_found_ids
# modifies self.twitter_users_to_notice_dates
# self.twitter_users_to_snapshots = {user_id: [snapshot]}
# self.user_ids_to_not_found_ids = {}
def test_get_users_to_snapshots():
    # load TwitterUserSnapshot, some with twitter_user_id, some with twitter_not_found_id
    load_test_db_records(CONFIG)

    # pre
    to = initialize_default_controller()
    to.twitter_users_to_notice_dates = to.get_users_to_notice_dates()
    to.user_ids_to_not_found_ids = {uid: None for uid in to.twitter_users_to_notice_dates.keys()}

    # test
    twitter_users_to_snapshots = to.get_users_to_snapshots()

    # asserts
    # make sure self.user_ids_to_not_found_ids is updated correctly
    # make sure uids get updated correctly in self.twitter_users_to_notice_dates
    assert len(twitter_users_to_snapshots) == ELIGIBLE_COUNT+NOT_ELIGIBLE_COUNT #
    assert len(to.user_ids_to_not_found_ids) == ELIGIBLE_COUNT+NOT_ELIGIBLE_COUNT #    

# in create_datasets(), sets twitter_users_to_tweets
# self.twitter_users_to_tweets = {user_id: [tweet_created_at_day]}
# "batch" method, so takes in list of this_uids 
def test_get_users_to_tweets():
    # load some users that have many tweets, others without
    load_test_db_records(CONFIG)

    # pre
    to = initialize_default_controller()
    to.twitter_users_to_notice_dates = to.get_users_to_notice_dates()
    to.user_ids_to_not_found_ids = {uid: None for uid in to.twitter_users_to_notice_dates.keys()}
    to.twitter_users_to_snapshots = to.get_users_to_snapshots()

    # test
    this_uids = [user["user_id"] for user in CONFIG] # [id for id in self.user_ids_to_not_found_ids if "NOT_FOUND" not in id]
    twitter_users_to_tweets = to.get_users_to_tweets(this_uids)

    # asserts
    assert len(twitter_users_to_tweets) == ELIGIBLE_COUNT+NOT_ELIGIBLE_COUNT #


# in create_datasets(), sets self.twitter_users_day_nums
# self.twitter_users_day_nums = 
#    {user_id: 
#       {day_num: 
#           {"num_notices": 0, 
#            "num_tweets": 0, 
#            "num_media_tweets": 0, 
#            "deleted": 0, 
#            "suspended": 0, 
#            "protected": 0
#    }}}
# "batch" method, so takes in list of this_uids
def test_get_users_day_nums():
    load_test_db_records(CONFIG)

    # pre
    to = initialize_default_controller()
    to.twitter_users_to_notice_dates = to.get_users_to_notice_dates()
    to.user_ids_to_not_found_ids = {uid: None for uid in to.twitter_users_to_notice_dates.keys()}
    to.twitter_users_to_snapshots = to.get_users_to_snapshots()
    log.info(to.twitter_users_to_snapshots)
    this_uids = [user["user_id"] for user in CONFIG] # [id for id in self.user_ids_to_not_found_ids if "NOT_FOUND" not in id]
    to.twitter_users_to_tweets = to.get_users_to_tweets(this_uids)

    # test
    twitter_users_day_nums = to.get_users_day_nums(this_uids)

    # asserts
    log.info(twitter_users_day_nums)
    assert len(twitter_users_day_nums) == ELIGIBLE_COUNT+NOT_ELIGIBLE_COUNT #

    # TODO: add more asserts here

# in create_datasets(), sets self.twitter_users_aggregates
# self.twitter_users_aggregates = 
#    {user_id: 
#        {
#            "total_unavailable_hours": 0,   # how to calculate this ????
#            "num_days_before_day_0": 0,
#            "num_days_after_day_0": 0,
#            "ave_tweets_before_day_0": 0,
#            "ave_tweets_after_day_0": 0,
#            "total_tweets": 0,
#            "account_suspended": False, # ever
#            "account_deleted": False, # ever
#            "account_protected": False, # ever         
#        }
# "batch" method, so takes in list of this_uids
# prunes: removes all info for users without min days. modifies self.user_ids_to_not_found_ids
def test_get_aggregates():
    load_test_db_records(CONFIG)

    # pre
    to = initialize_default_controller()
    to.twitter_users_to_notice_dates = to.get_users_to_notice_dates()
    to.user_ids_to_not_found_ids = {uid: None for uid in to.twitter_users_to_notice_dates.keys()}
    to.twitter_users_to_snapshots = to.get_users_to_snapshots()
    this_uids = [user["user_id"] for user in CONFIG] #
    to.twitter_users_to_tweets = to.get_users_to_tweets(this_uids)
    to.twitter_users_day_nums = to.get_users_day_nums(this_uids)

    # test
    twitter_users_aggregates = to.get_aggregates(this_uids)

    # asserts
    # assert calculations
    log.info(twitter_users_aggregates)
    assert len(twitter_users_aggregates) == ELIGIBLE_COUNT #

    # TODO: add more asserts here


# sets self.user_dataframe self.tweet_day_dataframe
# removes day nums outside of min days.
def test_create_dataframes():
    load_test_db_records(CONFIG)

    # pre
    to = initialize_default_controller()
    to.twitter_users_to_notice_dates = to.get_users_to_notice_dates()
    to.user_ids_to_not_found_ids = {uid: None for uid in to.twitter_users_to_notice_dates.keys()}
    to.twitter_users_to_snapshots = to.get_users_to_snapshots()
    this_uids = [user["user_id"] for user in CONFIG] #
    to.twitter_users_to_tweets = to.get_users_to_tweets(this_uids)
    to.twitter_users_day_nums = to.get_users_day_nums(this_uids)
    to.twitter_users_aggregates = to.get_aggregates(this_uids)

    # test
    to.create_dataframes(this_uids)

    # asserts
    assert len(to.user_dataframe) == ELIGIBLE_COUNT #
    assert len(to.tweet_day_dataframe) == ELIGIBLE_COUNT #

    # TODO: add more asserts here
