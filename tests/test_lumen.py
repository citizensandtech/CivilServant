import app.connections.lumen_connect
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

@patch('app.connections.lumen_connect.LumenConnect', autospec=True)
def test_archive_lumen_notices(mock_LumenConnect):
    lc = mock_LumenConnect.return_value
    with open("{script_dir}/fixture_data/lumen_notices_0.json".format(script_dir=TEST_DIR)) as f:
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
