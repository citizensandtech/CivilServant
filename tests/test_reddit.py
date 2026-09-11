import os

import pytest
from unittest.mock import patch

import reddit.connection
from app.models import PrawKey
from utils.common import DbEngine

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
ENV = os.environ["CS_ENV"] = "test"

db_session = DbEngine(
    os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)
).new_session()


def clear_praw_keys():
    db_session.query(PrawKey).delete()
    db_session.commit()


def setup_function(function):
    clear_praw_keys()


def teardown_function(function):
    clear_praw_keys()


@patch("reddit.connection.praw.Reddit")
def test_connect_reads_seeded_praw_key(mock_reddit):
    praw_id = PrawKey.get_praw_id(ENV, "Main")
    db_session.add(PrawKey(id=praw_id, refresh_token="seeded-refresh-token"))
    db_session.commit()

    conn = reddit.connection.Connect(db_session=db_session, env=ENV)
    r = conn.connect(controller="Main")

    mock_reddit.assert_called_once_with(refresh_token="seeded-refresh-token")
    assert r is mock_reddit.return_value
    assert db_session.query(PrawKey).count() == 1


@patch("reddit.connection.praw.Reddit")
def test_connect_falls_back_to_main_for_unknown_controller(mock_reddit):
    db_session.add(
        PrawKey(id=PrawKey.get_praw_id(ENV, "Main"), refresh_token="main-token")
    )
    db_session.commit()

    conn = reddit.connection.Connect(db_session=db_session, env=ENV)
    conn.connect(controller="SomeDynamicExperiment")

    mock_reddit.assert_called_once_with(refresh_token="main-token")


@patch("reddit.connection.praw.Reddit")
def test_connect_raises_without_any_praw_key(mock_reddit):
    conn = reddit.connection.Connect(db_session=db_session, env=ENV)
    with pytest.raises(RuntimeError):
        conn.connect(controller="Main")
    mock_reddit.assert_not_called()
