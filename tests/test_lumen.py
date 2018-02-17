import app.connections.lumen_connect
import app.controller
import app.controllers.lumen_controller
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
ENV = os.environ['CS_ENV'] ="test"

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

@patch('app.connections.lumen_connect.LumenConnect', autospec=True)
def test_archive_lumen_notices(mock_LumenConnect):
    lc = mock_LumenConnect.return_value
    with open("{script_dir}/fixture_data/anon_lumen_notices_0.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        lc.get_notices_to_twitter.return_value = json.loads(data)
    patch('app.connections.lumen_connect.')

    ## NOW START THE TEST
    from_date = datetime.datetime.utcnow() - datetime.timedelta(days=2)
    to_date = datetime.datetime.utcnow()
    data_json = lc.get_notices_to_twitter(["Copyright"], 50, 1, from_date, to_date)
    notices_json = data_json["notices"]
    assert len(notices_json) == 50
    assert type(notices_json[0]) == dict
    assert type(notices_json) == list

    ## TODO: COMPLETE THIS TEST


@patch('app.connections.lumen_connect.LumenConnect', autospec=True)
def test_parse_lumen_notices(mock_LumenConnect):
    lc = mock_LumenConnect.return_value
    with open("{script_dir}/fixture_data/anon_lumen_notices_0.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        lumen_return_data = json.loads(data)
        for notice in lumen_return_data['notices']:
            notice['date_received'] =  (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        lc.get_notices_to_twitter.return_value = lumen_return_data
    patch('app.connections.lumen_connect.')

    num_days = 2

    l = app.controllers.lumen_controller.LumenController(db_session, lc, log)
    date = datetime.datetime.utcnow() - datetime.timedelta(days=int(float(num_days)))
    l.archive_lumen_notices(['Copyright'], date)

    assert(len(db_session.query(LumenNotice).all()) == len(lumen_return_data['notices']))

    try:
        l.query_and_parse_notices_archive_users(test_exception=True)
    except:
        ## AT THIS POINT, WE HAVE THINGS THAT SHOULD BE "NOT PROCESSED" THAT ARE
        ## LABELED CS_JobState==2 (e.g. IN PROGRESS) DESPITE HAVING THE CODE FAIL
        notices = [x for x in db_session.query(LumenNotice).all()]
        for notice in notices:
            assert notice.CS_parsed_usernames != CS_JobState.IN_PROGRESS.value
        assert notices[0].CS_parsed_usernames != CS_JobState.NOT_PROCESSED.value
        assert notices[-1].CS_parsed_usernames == CS_JobState.NOT_PROCESSED.value
    else:
        assert False # expected query_and_parse_notices_archive_users to throw test_exception
