import snoonotes.connection
import os
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
from mock import Mock, patch
from app.models import Base, PrawKey
from utils.common import DbEngine

TEST_DIR = os.path.dirname(os.path.realpath(__file__))

os.environ['CS_ENV'] ="test"

def setup_function(function):
    pass

def teardown_function(function):
    pass

@patch('snoonotes.connection.SnooNotesConnect', autospec=True)
def test_get_users(mock_snoonotesconnect):
    snc = mock_snoonotesconnect.return_value
    with open("{script_dir}/fixture_data/snoonotes_users.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        snc.get_users.return_value = json.loads(data)
    patch('snoonotes.')

    ## NOW START THE TEST
    res_json = snc.get_users()
    assert type(res_json) == list
    assert len(res_json) == 1

@patch('snoonotes.connection.SnooNotesConnect', autospec=True)
def test_post_get_notes(mock_snoonotesconnect):
    snc = mock_snoonotesconnect.return_value
    users = ["natematias"]
    with open("{script_dir}/fixture_data/snoonotes_notes.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        snc.post_get_notes.return_value = json.loads(data)
    patch('snoonotes.')

    ## NOW START THE TEST
    res_json = snc.post_get_notes(users)
    assert type(res_json) == dict    
    assert len(res_json[users[0]]) == 3

@patch('snoonotes.connection.SnooNotesConnect', autospec=True)
def test_get_schemas(mock_snoonotesconnect):
    snc = mock_snoonotesconnect.return_value
    with open("{script_dir}/fixture_data/snoonotes_schemas.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        snc.get_schemas.return_value = json.loads(data)
    patch('snoonotes.')

    ## NOW START THE TEST
    res_json = snc.get_schemas()
    assert type(res_json) == list
    assert len(res_json) == 2