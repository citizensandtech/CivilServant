from contextlib import contextmanager
import glob
import simplejson as json
import os
import pytest
from unittest.mock import MagicMock, patch

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

import app.cs_logger
from app.models import *
from utils.common import DbEngine

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DIR = os.path.join(BASE_DIR, "tests")


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


class Helpers:
    """Helper methods that can be injected into tests."""

    @staticmethod
    def clear_all_tables(db_session):
        db_session.execute("UNLOCK TABLES")
        db_session.query(FrontPage).delete()
        db_session.query(SubredditPage).delete()
        db_session.query(Subreddit).delete()
        db_session.query(Post).delete()
        db_session.query(User).delete()
        db_session.query(ModAction).delete()
        db_session.query(Comment).delete()
        db_session.query(Experiment).delete()
        db_session.query(ExperimentThing).delete()
        db_session.query(ExperimentAction).delete()
        db_session.query(ExperimentThingSnapshot).delete()
        db_session.query(EventHook).delete()
        db_session.commit()

    @staticmethod
    @contextmanager
    def with_mock_reddit(modaction_data, create_redditor):
        """Mock the `praw.Reddit` class so it does not touch the network.
        Note that this should be used as a context manager in your test fixtures.

        Args:
            modaction_data: paginated fixture records.
            create_redditor: a function that takes a username and returns a `praw.Redditor`.

        Returns:
            A mocked `praw.Reddit` instance.
        """
        # Fixture data is broken up like this to allow testing of pagination in API results.
        # Always return a blank final page to ensure that our code thinks it's done pulling new results.
        # NOTE: Mock will return the next item in the array each time it's called.
        mod_log_pages = [
            modaction_data[i : i + 100] for i in range(0, len(modaction_data), 100)
        ] + [[]]

        with patch("praw.Reddit", autospec=True, spec_set=True) as reddit:
            reddit.get_mod_log = MagicMock(side_effect=mod_log_pages)
            reddit.get_redditor = MagicMock(side_effect=create_redditor)
            yield reddit

    @staticmethod
    def load_mod_actions(mod_controller, experiment_controller):
        """Load all fixture data, like `controller.fetch_mod_action_history` does.

        We're reimplementing it here for clarity, so we don't have to mock the function.
        """

        after_id, num_actions_stored = mod_controller.archive_mod_action_page()
        while num_actions_stored > 0:
            after_id, num_actions_stored = mod_controller.archive_mod_action_page(
                after_id
            )

        # NOTE: this also may store a subreddit ID that we don't want, short-circuiting matching logic.
        # To work around this, we hardcode it to always match.
        mod_controller.fetched_subreddit_id = experiment_controller.experiment_settings[
            "subreddit_id"
        ]


@pytest.fixture
def helpers():
    """To use a helper, include the `helpers` fixture in your test."""

    return Helpers
