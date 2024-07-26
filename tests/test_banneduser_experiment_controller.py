import pytest
import os
import yaml
import glob
import simplejson as json
import praw
from unittest.mock import Mock, patch

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

from app.controllers.banneduser_experiment_controller import (
    BanneduserExperimentController,
)
from app.controllers.moderator_controller import ModeratorController
import app.cs_logger
from utils.common import DbEngine
from app.models import *


TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.join(TEST_DIR, "../")


@pytest.fixture
def db_session():
    config_file = os.path.join(BASE_DIR, "config", f"{ENV}.json")
    return DbEngine(config_file).new_session()


@pytest.fixture
def logger():
    return app.cs_logger.get_logger(ENV, BASE_DIR)


def _clear_all_tables(db_session):
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


@pytest.fixture(autouse=True)
def with_setup_and_teardown(db_session):
    _clear_all_tables(db_session)
    yield
    _clear_all_tables(db_session)


@pytest.fixture
def modaction_fixtures():
    fixtures = []
    for filename in sorted(glob.glob(f"{TEST_DIR}/fixture_data/mod_actions*")):
        with open(filename, "r") as f:
            fixtures += json.load(f)
    return fixtures


@pytest.fixture
def reddit_return_value(modaction_fixtures):
    with patch("praw.Reddit", autospec=True) as mock_reddit:
        r = mock_reddit.return_value

        m = Mock()
        # Fixture data is broken up like this to allow testing of API 'pagination'
        m.side_effect = [
            modaction_fixtures[i : i + 100]
            for i in range(0, len(modaction_fixtures), 100)
        ] + [[]]
        r.get_mod_log = m

        return r


@pytest.fixture
def experiment_controller(db_session, reddit_return_value, logger):
    c = BanneduserExperimentController(
        "banneduser_experiment_test", db_session, reddit_return_value, logger
    )

    db_session.add(
        Subreddit(
            id=c.experiment_settings["subreddit_id"],
            name=c.experiment_settings["subreddit"],
        )
    )
    db_session.commit()

    return c


@pytest.fixture
def mod_controller(subreddit_name, db_session, reddit_return_value, logger):
    return ModeratorController(subreddit_name, db_session, reddit_return_value, logger)


@pytest.fixture
def subreddit_name(experiment_controller):
    return experiment_controller.experiment_settings["subreddit"]


@pytest.fixture
def subreddit_id(experiment_controller):
    return experiment_controller.experiment_settings["subreddit_id"]


@pytest.fixture
def log_filename(logger):
    file_handler = None
    for handler in logger.handlers:
        if type(handler).__name__ == "ConcurrentRotatingFileHandler":
            file_handler = handler
            break
    if not file_handler:
        assert False
    return handler.baseFilename


def _assert_logged(log_filename, text, max_lookback=1):
    lines_scanned = 0
    with open(log_filename, "r") as f:
        for line in reversed(list(f.readlines())):
            if text in line:
                return
            lines_scanned += 1
            if lines_scanned > max_lookback:
                assert False, f"Log text note found within {max_lookback} lines: {text}"
    assert False, "Log text not found: {text}"


def test_initialize_experiment(
    mod_controller, db_session, modaction_fixtures, log_filename
):
    assert db_session.query(ModAction).count() == 0

    # Archive only the first API result page.
    mod_controller.archive_mod_action_page()

    # Fixtures are split into 100-item pages, and we just loaded one page.
    assert db_session.query(ModAction).count() == 100

    _assert_logged(
        log_filename, "BanneduserExperimentController::enroll_new_participants"
    )


def test_load_all_fixtures(
    mod_controller, db_session, modaction_fixtures, log_filename
):
    # We have multiple API result pages of fixtures.
    assert len(modaction_fixtures) > 100

    assert db_session.query(ModAction).count() == 0

    # Load all fixtures, like `controller.fetch_mod_action_history`.
    after_id, num_actions_stored = mod_controller.archive_mod_action_page()
    while num_actions_stored > 0:
        after_id, num_actions_stored = mod_controller.archive_mod_action_page(after_id)

    assert db_session.query(ModAction).count() == len(modaction_fixtures)

    # Archiving multiple pages gets chatty in the logs, so we allow some entries to come after.
    # This may break if we add more logging. If that happens, increase the max_lookback setting.
    _assert_logged(
        log_filename,
        "BanneduserExperimentController::enroll_new_participants",
        max_lookback=12,
    )


# TODO: remove this test; it's only here to make assertions about the current test data.
def test_current_ban_fixtures(modaction_fixtures):
    ban_actions = [f for f in modaction_fixtures if f["action"] == "banuser"]
    assert len(ban_actions) == 3


# The following are testing the modaction experiment controller:


def test_get_condition(experiment_controller):
    assert experiment_controller._get_condition() == "main"


def test_start_with_empty_enrollment(experiment_controller):
    assert experiment_controller._previously_enrolled_user_ids() == []


def test_other_experiment_not_detected_as_enrolled(experiment_controller):
    et = ExperimentThing(
        id="et1",
        thing_id="123",
        experiment_id=9999,
        object_type=ThingType.USER.value,
    )
    experiment_controller.db_session.add(et)
    experiment_controller.db_session.commit()
    assert experiment_controller._previously_enrolled_user_ids() == []


def test_other_type_not_detected_as_enrolled(experiment_controller):
    et = ExperimentThing(
        id="et1",
        thing_id="456",
        experiment_id=experiment_controller.experiment.id,
        object_type=ThingType.COMMENT.value,
    )
    experiment_controller.db_session.add(et)
    experiment_controller.db_session.commit()
    assert experiment_controller._previously_enrolled_user_ids() == []


def test_user_detected_as_enrolled(experiment_controller):
    et = ExperimentThing(
        id="et1",
        thing_id="123",
        experiment_id=experiment_controller.experiment.id,
        object_type=ThingType.USER.value,
    )
    experiment_controller.db_session.add(et)
    experiment_controller.db_session.commit()
    assert experiment_controller._previously_enrolled_user_ids() == ["123"]
