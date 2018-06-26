import pytest
import os
from mock import Mock, patch
#import simplejson as json
import json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
import glob, datetime
from app.controllers.messaging_controller import *
from utils.common import PageType, DbEngine, json2obj

### LOAD THE CLASSES TO TEST
from app.models import MessageLog
import app.cs_logger

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] ="test"

db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()

def clear_all_tables():
    db_session.query(MessageLog).delete()      
    db_session.commit()    

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()

@patch('praw.Reddit', autospec=True)
def test_send_message(mock_reddit):
    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

    mock_test_message_recipient = "CivilServantBot"

    ## TEST THE BASE CASE:
    r.send_message.return_value = {"errors":[]}
    patch('praw.')
    mc = MessagingController(db_session, r, log)
    result = mc.send_message(mock_test_message_recipient, "test message body", "test subject", "test message", '{}')
    assert len(result['errors']) == 0
    assert db_session.query(MessageLog).count() == 1
    ml = db_session.query(MessageLog).first()
    for key in ['platform', 'username', 'subject', 'body', 'message_task_id']:
        assert getattr(ml, key) != None
    log_metadata = json.loads(ml.metadata_json)
    assert ml.message_task_id == "test message"

    ## TEST ERROR HANDLING
    r.send_message.return_value = {"errors":[{"username":mock_test_message_recipient, "error":"simulating an error"}]}
    patch('praw.')
    mc = MessagingController(db_session, r, log)
    result = mc.send_message(mock_test_message_recipient, "test message", "test subject", "test messages")
    assert len(result['errors']) == 1
    assert db_session.query(MessageLog).count() == 2

@patch('praw.Reddit', autospec=True)
def test_send_messages(mock_reddit):
    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)
    mc = MessagingController(db_session, r, log)
    mock_test_message_recipient = "CivilServantBot2"

    ## Mock receiving a series of responses, no errors
    r.send_message.side_effect = [
        {"errors":[]},
        {"errors":[]},
        {"errors":[]} ]
    patch('praw.')

    messages = [
        {"account":"natematias", "subject":"natematias subject", "message": "natematias message"},
        {"account":"CivilServantBot", "subject":"CivilServantBot subject", "message": "CivilServantBot message"},
        {"account":"CivilServantBot2", "subject":"CivilServantBot2 subject", "message": "CivilServantBot2 message"},
    ]

    log_results = mc.send_messages(messages, "test messages")
    assert len(log_results) == len(messages)
    assert db_session.query(MessageLog).count() == len(messages)

    message_log_count = len(messages)

    ## Mock the case where duplicate account names are sent as messages 
    r.send_message.side_effect = [
        {"errors":[]},
        {"errors":[]},
        {"errors":[]} ]
    patch('praw.')
    messages = [
        {"account":"natematias", "subject":"natematias subject", "message": "natematias message"},
        {"account":"natematias", "subject":"natematias subject", "message": "natematias message"},
        {"account":"natematias", "subject":"natematias subject", "message": "natematias message"}
    ]

    with pytest.raises(MessageError) as excinfo:
        log_results = mc.send_messages(messages, "test messages overlap")
    assert excinfo.value.args[0][0] == 'Duplicate accounts submitted to send_messages.'
    assert excinfo.value.args[0][1] == {"natematias": 3}
    
    assert db_session.query(MessageLog).count() == message_log_count

    ## Mock the case where at least one attempt to send a message results in an error
    error_message = "simulating an error"
    r.send_message.side_effect = [
        {"errors":[]},
        {"errors":[]},
        {"errors": [
            {"username":mock_test_message_recipient, "error":error_message}
        ]}
    ]
    patch('praw.')

    messages = [
        {"account":"natematias", "subject":"natematias subject", "message": "natematias message"},
        {"account":"CivilServantBot", "subject":"CivilServantBot subject", "message": "CivilServantBot message"},
        {"account":mock_test_message_recipient, "subject":"CivilServantBot2 subject", "message": "CivilServantBot2 message"},
    ]

    one_error_task_id = "test messages with one error"
    log_results = mc.send_messages(messages, one_error_task_id)
    assert len(log_results) == 3
    assert log_results[mock_test_message_recipient]['errors'][0]['error'] == error_message

    ## now check the database for a log of the error
    failed_message_logs = db_session.query(MessageLog).filter(
                            MessageLog.message_sent==False).all()
    assert len(failed_message_logs) == 1
    failed_message_log = failed_message_logs[0]
    assert failed_message_log.message_sent == False
    assert failed_message_log.username == mock_test_message_recipient
    assert failed_message_log.message_task_id == one_error_task_id



