import glob
import simplejson as json
import os
import pytest

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

from utils.common import DbEngine

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DIR = os.path.join(BASE_DIR, "tests")

import app.cs_logger


@pytest.fixture
def db_session():
    config_file = os.path.join(BASE_DIR, "config", f"{ENV}.json")
    return DbEngine(config_file).new_session()


@pytest.fixture
def logger():
    # The logger is passed as an argument to various constructors so we need an instance ready.
    return app.cs_logger.get_logger(ENV, BASE_DIR)


@pytest.fixture
def modaction_data():
    actions = []
    for filename in sorted(glob.glob(f"{TEST_DIR}/fixture_data/mod_actions*")):
        with open(filename, "r") as f:
            actions += json.load(f)
    return actions
