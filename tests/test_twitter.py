from time import sleep

import pytest
import app.connections.twitter_connect
#import app.controller
import app.controllers.twitter_controller
import os
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
from mock import Mock, patch
from app.models import *
from utils.common import *
import app.cs_logger

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] = "test"

db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)

def clear_twitter_tables():
    for table in (TwitterRateState, TwitterToken):
        db_session.query(table).delete()
        db_session.commit()

def setup_function(function):
    clear_twitter_tables()


def teardown_function(function):
    clear_twitter_tables()
    db_session.query(LumenNotice).delete()
    db_session.query(LumenNoticeExpandedURL).delete()
    db_session.query(LumenNoticeToTwitterUser).delete()
    db_session.query(TwitterUser).delete()
    db_session.query(TwitterUserSnapshot).delete()
    db_session.query(TwitterStatus).delete()
    db_session.commit()

@pytest.fixture
def populate_notice_users():
    fixture_dir = os.path.join(TEST_DIR, "fixture_data")
    with open("{script_dir}/fixture_data/notice_user.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        noticeuser_data = json.loads(data)
        now = datetime.datetime.utcnow()

        for nu in noticeuser_data:
            notice_user_record = LumenNoticeToTwitterUser(
                    record_created_at = now,
                    notice_id = nu["notice_id"],
                    twitter_username = nu["twitter_username"],
                    twitter_user_id = None,
                    CS_account_archived = CS_JobState.NOT_PROCESSED.value
                )
            db_session.add(notice_user_record)
        db_session.commit()

@patch('twitter.Api', autospec=True)
def test_archive_twitter_new_users(mock_twitter, populate_notice_users):
    log.info("STARTING test_archive_twitter_new_users")
    t = mock_twitter.return_value
    before_creation = datetime.datetime.now()
    sleep(1.5)

    with open("{script_dir}/fixture_data/anon_users_lookup_0.json".format(script_dir=TEST_DIR)) as f:
        fixture = json.loads(f.read())
        t.UsersLookup.__name__ = 'UsersLookup'
        t.UsersLookup.return_value = fixture

    conn = app.connections.twitter_connect.TwitterConnect(log=log, db_session=db_session)
    t_ctrl = app.controllers.twitter_controller.TwitterController(db_session, conn, log)

    try:
        t_ctrl.query_and_archive_new_users(test_exception=True)
    except:
        noticeusers = [x for x in db_session.query(LumenNoticeToTwitterUser).all()]
        for noticeuser in noticeusers:
            assert noticeuser.CS_account_archived != CS_JobState.IN_PROGRESS.value
    else:
        assert False # expected query_and_archive_new_users to throw test_exception

    sleep(1.5)
    expiration = datetime.datetime.now()
    ratestates = db_session.query(TwitterRateState).filter(TwitterRateState.endpoint == '/users/lookup')

    for ratestate in ratestates:
        assert ratestate.checkin_due > before_creation
        assert ratestate.checkin_due < expiration

@patch('twitter.Api', autospec=True)
def test_with_user_records_archive_tweets(mock_twitter_api):
    tc = app.connections.twitter_connect.TwitterConnect(log=log, db_session=db_session)
    api = mock_twitter_api.return_value

    def mocked_GetUserTimeline(user_id, count=None, max_id=None):
        with open("{script_dir}/fixture_data/anon_twitter_tweets.json".format(script_dir=TEST_DIR)) as f:
            data = json.loads(f.read())
        assert len(data) == 200
        if user_id == "2" or user_id == "3": # suspended_user or protected_user
            raise twitter.error.TwitterError("Not authorized.") # not mocking TwitterError
        elif user_id == "1": # deleted_user
            raise twitter.error.TwitterError([{'message': 'Sorry, that page does not exist.', 'code': 34}])
        else:   # # existing_user ?
            return data


    m = Mock()
    m.side_effect = mocked_GetUserTimeline
    api.GetUserTimeline = m
    api.GetUserTimeline.__name__ = "GetUserTimeline"
    tc.api = api
    patch('twitter.')

    assert len(db_session.query(TwitterStatus).all()) == 0

    t_controller = app.controllers.twitter_controller.TwitterController(db_session, tc, log)

    user_results = [
        ({"screen_name": "existing_user", "id": "888", "user_state": TwitterUserState.FOUND.value}, {"status_count": 200, "user_state": TwitterUserState.FOUND.value}),
        ({"screen_name": "deleted_user", "id": "1", "user_state": TwitterUserState.NOT_FOUND.value}, {"status_count": 0, "user_state": TwitterUserState.NOT_FOUND.value}),
        ({"screen_name": "suspended_user", "id": "2", "user_state": TwitterUserState.NOT_FOUND.value}, {"status_count": 0, "user_state": TwitterUserState.SUSPENDED.value}),
        ({"screen_name": "protected_user", "id": "3", "user_state": TwitterUserState.PROTECTED.value}, {"status_count": 0, "user_state": TwitterUserState.PROTECTED.value})
    ]

    user_records = []
    for i, (user, result) in enumerate(user_results):
        # need to create TwitterUser records first
        user_record = TwitterUser(
            id = user["id"],
            screen_name = user["screen_name"],
            user_state = user["user_state"])
        db_session.add(user_record)
        db_session.commit()
        user_records.append(user_record)

    try:
        t_controller.with_user_records_archive_tweets(user_records, backfill=True, is_test=True)
    except:
        user_records = [x for x in db_session.query(TwitterUser).all()]
        for user_record in user_records:
            assert user_record.CS_oldest_tweets_archived != CS_JobState.IN_PROGRESS.value
        assert len([x for x in user_records if x.CS_oldest_tweets_archived == CS_JobState.PROCESSED.value]) > 0
    else:
        assert False # expected query_and_archive_new_users to throw test_exception

