from dataclasses import dataclass, field
import os
import glob
import simplejson as json
from unittest.mock import MagicMock, Mock, patch

from praw.objects import Redditor
import pytest
import simplejson as json

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

from app.controllers.banneduser_experiment_controller import (
    BanneduserExperimentController,
)
import app.cs_logger
from app.controllers.moderator_controller import ModeratorController
from utils.common import DbEngine
from app.models import *

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.join(TEST_DIR, "../")


@pytest.fixture
def db_session():
    config_file = os.path.join(BASE_DIR, "config", f"{ENV}.json")
    return DbEngine(config_file).new_session()


@pytest.fixture(autouse=True)
def with_setup_and_teardown(db_session):
    def _clear_all_tables():
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

    _clear_all_tables()
    yield
    _clear_all_tables()


@pytest.fixture
def modaction_data():
    actions = []
    for filename in sorted(glob.glob(f"{TEST_DIR}/fixture_data/mod_actions*")):
        with open(filename, "r") as f:
            actions += json.load(f)
    return actions


@pytest.fixture
def fake_get_mod_log(modaction_data):
    # Fixture data is broken up like this to allow testing of pagination in API results.
    # Always return a blank final page to ensure that our code thinks it's done pulling new results.
    # NOTE: Mock will return the next item in the array each time it's called.
    mod_log_pages = [
        modaction_data[i : i + 100] for i in range(0, len(modaction_data), 100)
    ] + [[]]

    return MagicMock(side_effect=mod_log_pages)


@pytest.fixture
def fake_get_redditor():
    # Generate a fake Redditor based only on the username.
    # This could look up a record, but so far we don't need to do that.
    def create_fake_redditor(username):
        user_data = {"created_utc": 9999}
        user = Redditor(MagicMock(), user_name=username, json_dict=user_data)
        return user

    yield MagicMock(side_effect=create_fake_redditor)


@pytest.fixture
def fake_reddit(fake_get_mod_log, fake_get_redditor):
    with patch("praw.Reddit", autospec=True, spec_set=True) as reddit:
        reddit.get_mod_log = fake_get_mod_log
        reddit.get_redditor = fake_get_redditor
        yield reddit


@pytest.fixture
def logger():
    return app.cs_logger.get_logger(ENV, BASE_DIR)


@pytest.fixture
def experiment_controller(db_session, fake_reddit, logger):
    c = BanneduserExperimentController(
        "banneduser_experiment_test", db_session, fake_reddit, logger
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
def moderator_controller(db_session, fake_reddit, logger, experiment_controller):
    return ModeratorController(
        experiment_controller.experiment_settings["subreddit"],
        db_session,
        fake_reddit,
        logger,
    )


class TestRedditMock:
    def test_fake_mod_log_first_page(self, fake_reddit):
        page = fake_reddit.get_mod_log("fake_subreddit")
        assert len(page) == 100

    def test_fake_mod_log_all_pages(self, fake_reddit):
        page = fake_reddit.get_mod_log("fake_subreddit")
        while page:
            assert len(page) > 1
            page = fake_reddit.get_mod_log("fake_subreddit")
        assert len(page) == 0

    def test_fake_get_redditor(self, fake_reddit):
        user = fake_reddit.get_redditor("uncivil")
        assert user.created_utc > 0
        assert user.name == "uncivil"


class TestModeratorController:
    def test_archive_mod_action_page(self, db_session, moderator_controller):
        assert db_session.query(ModAction).count() == 0

        # Archive the first API result page.
        _, unique_count = moderator_controller.archive_mod_action_page()
        assert unique_count == 100

        # Fixtures are split into 100-item pages, and we just loaded one page.
        assert db_session.query(ModAction).count() == 100

    def test_load_all_fixtures(self, db_session, modaction_data, moderator_controller):
        # Nothing is loaded yet.
        assert db_session.query(ModAction).count() == 0

        # We have multiple API result pages of fixtures.
        assert len(modaction_data) > 100

        # Load all fixture data, like `controller.fetch_mod_action_history` does.
        # We're reimplementing it here for clarity, so we don't have to mock the function.
        after_id, num_actions_stored = moderator_controller.archive_mod_action_page()
        while num_actions_stored > 0:
            after_id, num_actions_stored = moderator_controller.archive_mod_action_page(
                after_id
            )

        # All fixture records were loaded.
        assert db_session.query(ModAction).count() == len(modaction_data)


class TestExperimentController:
    def test_enroll_new_participants(self, experiment_controller, moderator_controller):
        assert len(experiment_controller._previously_enrolled_user_ids()) == 0

        experiment_controller.enroll_new_participants(moderator_controller)

        # FIXME someone should be enrolled now?
        # assert len(experiment_controller._previously_enrolled_user_ids()) > 0

    def test_update_experiment(self, experiment_controller):
        experiment_controller.update_experiment()

        # FIXME integration assertions


class TestPrivateMethods:
    # NOTE: experiment_id None will populate the current experiment ID.
    @pytest.mark.parametrize(
        "thing_id,experiment_id,object_type,want",
        [
            ("123", 9999, ThingType.USER.value, []),
            ("456", None, ThingType.COMMENT.value, []),
            ("123", None, ThingType.USER.value, ["123"]),
        ],
    )
    def test_previously_enrolled_user_ids(
        self, thing_id, experiment_id, object_type, want, experiment_controller
    ):
        et = ExperimentThing(
            id="et1",
            thing_id=thing_id,
            experiment_id=experiment_id or experiment_controller.experiment.id,
            object_type=object_type,
        )
        experiment_controller.db_session.add(et)
        experiment_controller.db_session.commit()
        assert experiment_controller._previously_enrolled_user_ids() == want

    def test_find_eligible_newcomers(self):
        pytest.fail()

    def test_update_existing_participants(self):
        pytest.fail()

    def test_get_account_age(self):
        pytest.fail()

    def test_get_condition(self):
        pytest.fail()

    def test_assign_randomized_conditions(self):
        pytest.fail()

    @pytest.mark.parametrize(
        "action,details,want",
        [
            ("banuser", "3 days", True),
            ("banuser", "1 day", False),  # They always use "days"
            ("removecomment", "remove", False),
            ("banuser", "permanent", False),
        ],
    )
    def test_is_tempban(self, action, details, want, experiment_controller):
        got = experiment_controller._is_tempban({"action": action, "details": details})
        assert got == want

    @pytest.mark.parametrize(
        "username,choices,want",
        [
            ("me", ["me", "you"], True),
            ("me", ["you", "them"], False),
            ("you", ["me", "them"], False),
        ],
    )
    def test_is_enrolled(self, username, choices, want, experiment_controller):
        got = experiment_controller._is_enrolled({"target_author": username}, choices)
        assert got == want

    @pytest.mark.parametrize(
        "username,want",
        [
            ("innocent_user", False),
            ("evilbot", True),
            ("HappyFunBot", True),
            ("BotALicious", False),
            ("FrancoisTalbot", True),  # LOL
        ],
    )
    def test_is_bot(self, username, want, experiment_controller):
        got = experiment_controller._is_bot({"target_author": username})
        assert got == want

    @pytest.mark.parametrize(
        "details,want",
        [
            (
                "2 days",
                {
                    "ban_duration_days": 2,
                    "ban_reason": "testing",
                    "ban_start_time": 33,
                    "ban_type": "temporary",
                    "ban_end_time": 172833,
                },
            ),
            ("bogus", {}),
        ],
    )
    def test_parse_temp_ban(self, details, want, experiment_controller):
        got = experiment_controller._parse_temp_ban(
            {
                "action": "banuser",
                "created_utc": 33,
                "description": "testing",
                "details": details,
            }
        )
        assert got == want

    @pytest.mark.parametrize(
        "action,details,want",
        [
            ("banuser", "1 days", 1),
            ("banuser", "7 days", 7),
            ("banuser", "forever", None),
            ("unbanuser", "3 days", None),
        ],
    )
    def test_parse_days(self, action, details, want, experiment_controller):
        got = experiment_controller._parse_days({"action": action, "details": details})
        assert got == want

    def test_get_accounts_needing_interventions(self):
        pytest.fail()

    def test_format_intervention_message(self):
        pytest.fail()

    def test_send_intervention_messages(self):
        pytest.fail()
