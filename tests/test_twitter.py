import pytest
import app.connections.twitter_connect
import app.controller
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

def setup_function(function):
    pass

def teardown_function(function):
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

# @patch('app.connections.twitter_connect.TwitterConnect', autospec=True)
@patch('twitter.Api', autospec=True)
def test_archive_twitter_new_users(mock_twitter, populate_notice_users):
    t = mock_twitter.return_value

    with open("{script_dir}/fixture_data/anon_users_lookup_0.json".format(script_dir=TEST_DIR)) as f:
        fixture = json.loads(f.read())
        t.UsersLookup.return_value = fixture

    conn = app.connections.twitter_connect.TwitterConnect()
    t_ctrl = app.controllers.twitter_controller.TwitterController(db_session, conn, log)

    try:
        t_ctrl.query_and_archive_new_users(test_exception=True)
    except:
        noticeusers = [x for x in db_session.query(LumenNoticeToTwitterUser).all()]
        for noticeuser in noticeusers:
            assert noticeuser.CS_account_archived != CS_JobState.IN_PROGRESS.value
    else:
        assert False # expected query_and_archive_new_users to throw test_exception
