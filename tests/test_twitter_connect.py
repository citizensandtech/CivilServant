import pytest
import os
import twitter
import datetime, time
from mock import Mock, patch
import simplejson as json
from utils.common import json2obj

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] ="test"

import app.connections.twitter_connect

def setup_function(function):
    pass

def teardown_function(function):
    pass

@patch('twitter.Api', autospec=True)
def test_twitter_connect(mock_twitter):
    t = mock_twitter.return_value
    conn = app.connections.twitter_connect.TwitterConnect()
    friend_accounts = []
    with open("{script_dir}/fixture_data/twitter_get_friends.json".format(script_dir=TEST_DIR)) as f:
        fixture = json.loads(f.read())
        for account in fixture:
            json_dump = json.dumps(account)
            account_obj = json2obj(json_dump)
            friend_accounts.append(account_obj)
    
    t.GetFriends.return_value = friend_accounts
    
    friends = conn.query(conn.api.GetFriends)
    assert len(friends)  == len(friend_accounts)

@patch('twitter.Api', autospec=True)
@patch('twitter.ratelimit.RateLimit', autospec=True)
def test_exception_retry(mock_rate_limit, mock_twitter):
    #TODO: In the unlikelihood that a VERY slow machine is running these tests
    # you can increase the timedelta here and below to microseconds=500
    reset_time = (datetime.datetime.now() + datetime.timedelta(seconds=1))
    mock_rate_limit.resources = {"getfriends":{"/friends/list":{
        "reset":time.mktime(reset_time.timetuple()),
        "remaining":0,
        "limit":15}}}

    t = mock_twitter.return_value
    t.rate_limit = mock_rate_limit
    t.VerifyCredentials.return_value = True

    conn = app.connections.twitter_connect.TwitterConnect()

    friend_accounts = []
    with open("{script_dir}/fixture_data/twitter_get_friends.json".format(script_dir=TEST_DIR)) as f:
        fixture = json.loads(f.read())
        for account in fixture:
            json_dump = json.dumps(account)
            account_obj = json2obj(json_dump)
            friend_accounts.append(account_obj)

    t.GetFriends.side_effect = [twitter.error.TwitterError([{'code': 88, 'message': 'Rate limit exceeded'}]), friend_accounts]
    assert conn.token['user_id'] == 1
    friends = conn.query(conn.api.GetFriends)
    assert len(friends)  == len(friend_accounts)
    assert conn.token['user_id'] == 2

    ##now make it wait to go back to the previous key
    t.GetFriends.side_effect = [twitter.error.TwitterError([{'code': 88, 'message': 'Rate limit exceeded'}]), friend_accounts]
    mock_rate_limit.resources = {"getfriends":{"/friends/list":{
        "reset":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=1)).timetuple()),
        "remaining":0,
        "limit":15}}}
    t.rate_limit = mock_rate_limit

    assert conn.token['user_id'] == 2
    assert (reset_time - datetime.datetime.now()).total_seconds() > 0
    friends = conn.query(conn.api.GetFriends)
    assert len(friends)  == len(friend_accounts)
    assert conn.token['user_id'] == 1
    assert (reset_time - datetime.datetime.now()).total_seconds() < 0
