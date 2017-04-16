import lumen_connect.connection
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


# TODO: write tests
@patch('lumen_connect.connection.LumenConnect', autospec=True)
def test_archive_lumen_notices(mock_LumenConnect):
    lc = mock_LumenConnect.return_value
    with open("{script_dir}/fixture_data/lumen_notices.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        lc.get_search.return_value = json.loads(data)
    patch('lumen_connect.')

    ## NOW START THE TEST
    payload = {
        "topics": ["Copyright"],
        "per_page": 50,
        "page": 1,
        "sort_by": "date_received desc",
        "recipient_name": "Twitter"
    }    
    data_json = lc.get_search(payload)
    notices_json = data_json["notices"]
    assert type(notices_json) == list
    assert len(notices_json) == 50

"""
@patch('lumen.connection.LumenConnect', autospec=True)
def test_post_get_notes(mock_Lumenconnect):
    snc = mock_Lumenconnect.return_value
    users = ["natematias", "merrymou"]
    with open("{script_dir}/fixture_data/lumen_notes.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        snc.post_get_notes.return_value = json.loads(data)
    patch('lumen.')

    ## NOW START THE TEST
    res_json = snc.post_get_notes(users)
    assert type(res_json) == dict    
    assert len(res_json) == 2
    assert len(res_json[users[0]]) == 3

@patch('lumen.connection.LumenConnect', autospec=True)
def test_get_schemas(mock_Lumenconnect):
    snc = mock_Lumenconnect.return_value
    with open("{script_dir}/fixture_data/lumen_schemas.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        snc.get_schemas.return_value = json.loads(data)
    patch('lumen.')

    ## NOW START THE TEST
    res_json = snc.get_schemas()
    assert type(res_json) == list
    assert len(res_json) == 2
"""