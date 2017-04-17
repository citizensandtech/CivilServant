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

@patch('lumen_connect.connection.LumenConnect', autospec=True)
def test_archive_lumen_notices(mock_LumenConnect):
    lc = mock_LumenConnect.return_value
    with open("{script_dir}/fixture_data/lumen_notices_0.json".format(script_dir=TEST_DIR)) as f:
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