import datetime
import os
from unittest.mock import MagicMock, patch

from praw.objects import Redditor
import pytest

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

from app.controllers.banneduser_experiment_controller import (
    BanneduserExperimentController,
    BannedUserQueryIndex,
)
from app.controllers.experiment_controller import ExperimentConfigurationError
from app.controllers.moderator_controller import ModeratorController
from app.models import *


@pytest.fixture(autouse=True)
def with_setup_and_teardown(helpers, db_session):
    helpers.clear_all_tables(db_session)
    yield
    helpers.clear_all_tables(db_session)


@pytest.fixture
def mock_reddit(helpers, modaction_data):
    def create_redditor(username):
        """Generate a Redditor based only on the username.
        This could look up a record, but so far we don't need to do that.
        """
        user_data = {"created_utc": 9999}
        user = Redditor(MagicMock(), user_name=username, json_dict=user_data)
        return user

    with helpers.with_mock_reddit(modaction_data, create_redditor) as reddit:
        yield reddit


@pytest.fixture
def experiment_controller(db_session, mock_reddit, logger):
    # Create a controller instance with accompanying seed data for an experiment.
    c = BanneduserExperimentController(
        "banneduser_experiment_test", db_session, mock_reddit, logger
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
def mod_controller(db_session, mock_reddit, logger, experiment_controller):
    return ModeratorController(
        experiment_controller.experiment_settings["subreddit"],
        db_session,
        mock_reddit,
        logger,
    )


@pytest.fixture
def static_now():
    return datetime.datetime.utcnow().timestamp()


@pytest.fixture
def newcomer_modactions(static_now, experiment_controller, modaction_data):
    return experiment_controller._find_eligible_newcomers(static_now, modaction_data)


class TestRedditMock:
    """NOTE: The reddit mock is part of a higher level test structure, beyond the banned user experiment."""

    def test_fake_mod_log_first_page(self, mock_reddit):
        page = mock_reddit.get_mod_log("fake_subreddit")
        assert len(page) == 100

    def test_fake_mod_log_all_pages(self, mock_reddit):
        page = mock_reddit.get_mod_log("fake_subreddit")
        while page:
            assert len(page) > 1
            page = mock_reddit.get_mod_log("fake_subreddit")
        assert len(page) == 0

    def test_get_redditor(self, mock_reddit):
        user = mock_reddit.get_redditor("uncivil")
        assert user.created_utc > 0
        assert user.name == "uncivil"


class TestModeratorController:
    def test_archive_mod_action_page(self, db_session, mod_controller):
        assert db_session.query(ModAction).count() == 0

        # Archive the first API result page.
        _, unique_count = mod_controller.archive_mod_action_page()
        assert unique_count == 100

        # Fixtures are split into 100-item pages, and we just loaded one page.
        assert db_session.query(ModAction).count() == 100

    def test_load_all_fixtures(
        self, helpers, db_session, modaction_data, mod_controller, experiment_controller
    ):
        # Nothing is loaded yet.
        assert db_session.query(ModAction).count() == 0

        # We have multiple API result pages of fixtures.
        assert len(modaction_data) > 100

        helpers.load_mod_actions(mod_controller, experiment_controller)

        # All fixture records were loaded.
        assert db_session.query(ModAction).count() == len(modaction_data)


class TestExperimentController:
    def test_enroll_new_participants(
        self, helpers, experiment_controller, mod_controller
    ):
        assert len(experiment_controller._previously_enrolled_user_ids()) == 0

        helpers.load_mod_actions(mod_controller, experiment_controller)
        experiment_controller.enroll_new_participants(mod_controller)

        assert len(experiment_controller._previously_enrolled_user_ids()) > 0

    def test_update_experiment(self, experiment_controller):
        experiment_controller.update_experiment()
        # FIXME add integration assertions


class TestModactionPrivateMethods:
    def test_check_condition(self, experiment_controller):
        experiment_controller._check_condition("newcomer")
        experiment_controller._check_condition("experienced")
        with pytest.raises(Exception):
            experiment_controller._check_condition("reanimated")

    def test_previously_enrolled_user_ids(self, experiment_controller):
        assert experiment_controller._previously_enrolled_user_ids() == []

        et = ExperimentThing(
            id="prevuser",
            thing_id="12345",
            experiment_id=experiment_controller.experiment.id,
            object_type=ThingType.USER.value,
        )
        experiment_controller.db_session.add(et)
        experiment_controller.db_session.commit()

        assert experiment_controller._previously_enrolled_user_ids() == ["12345"]


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

    def test_find_eligible_newcomers(self, modaction_data, experiment_controller, static_now):
        # NOTE: not using newcomer_modactions for extra clarity.
        user_modactions = experiment_controller._find_eligible_newcomers(static_now, modaction_data)
        assert len(user_modactions) > 0

    # update temp ban duration
    @pytest.mark.parametrize(
        "action,details,want_duration,want_query_index,want_type",
        [
            ("banuser", "999 days", 999, BannedUserQueryIndex.PENDING, "temporary"),
            ("banuser", "permaban", None, BannedUserQueryIndex.IMPOSSIBLE, "permanent"),
            (
                "unbanuser",
                "whatever",
                None,
                BannedUserQueryIndex.IMPOSSIBLE,
                "unbanned",
            ),
        ],
    )
    def test_update_existing_participants(
        self,
        helpers,
        action,
        details,
        want_duration,
        want_query_index,
        want_type,
        newcomer_modactions,
        experiment_controller,
        mod_controller,
    ):
        helpers.load_mod_actions(mod_controller, experiment_controller)
        experiment_controller.enroll_new_participants(mod_controller)

        original = newcomer_modactions[0]
        update = {**original, "action": action, "details": details}
        experiment_controller._update_existing_participants([update])

        snap = (
            experiment_controller.db_session.query(ExperimentThingSnapshot)
            .filter(
                ExperimentThingSnapshot.experiment_thing_id == original["target_author"]
            )
            .first()
        )
        assert snap.object_type == ThingType.USER.value
        assert snap.experiment_id == experiment_controller.experiment.id
        meta = json.loads(snap.metadata_json)
        assert meta["ban_type"] == "temporary"

        user = (
            experiment_controller.db_session.query(ExperimentThing)
            .filter(ExperimentThing.thing_id == original["target_author"])
            .one()
        )
        assert user is not None
        assert user.query_index == want_query_index

        meta = json.loads(user.metadata_json)
        if want_duration:
            assert meta["ban_duration_days"] == want_duration
        assert meta["ban_type"] == want_type

    def test_get_condition(self, experiment_controller):
        experiment_controller._get_condition()

    def test_assign_randomized_conditions(self, modaction_data, experiment_controller, static_now):
        assert len(experiment_controller._previously_enrolled_user_ids()) == 0

        user_modactions = experiment_controller._find_eligible_newcomers(static_now, modaction_data)
        experiment_controller._assign_randomized_conditions(static_now, user_modactions)

        assert len(experiment_controller._previously_enrolled_user_ids()) > 1

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
        "action,details,want",
        [
            ("banuser", "3 days", False),
            ("banuser", "permanent", False),
            ("removecomment", "remove", False),
            ("banuser", "changed to 1 days", True),
            ("banuser", "changed to 3 days", True),
        ],
    )
    def test_is_tempban_edit(self, action, details, want, experiment_controller):
        got = experiment_controller._is_tempban_edit({"action": action, "details": details})
        assert got == want

    @pytest.mark.parametrize(
        "action,details,want",
        [
            ("banuser", "1 days", False),
            ("banuser", "2 days", False),
            ("banuser", "3 days", True),
            ("banuser", "7 days", True),
            ("banuser", "14 days", True),
            ("banuser", "30 days", True),
        ],
    )
    def test_is_valid_tempban_duration(self, action, details, want, experiment_controller):
        got = experiment_controller._is_valid_tempban_duration({"action": action, "details": details})
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

    def test_get_accounts_needing_interventions(
        self, helpers, experiment_controller, mod_controller
    ):
        users = experiment_controller._get_accounts_needing_interventions()
        assert users == []

        helpers.load_mod_actions(mod_controller, experiment_controller)
        experiment_controller.enroll_new_participants(mod_controller)

        users = experiment_controller._get_accounts_needing_interventions()
        assert len(users) > 0

    @pytest.mark.parametrize(
        "thing_id,metadata_json,want",
        [
            (
                "12345",
                {
                    "condition": "newcomer",
                    "arm": "arm_0",
                },
                {
                    "subject": "PM Subject Line for 12345 (Newcomer Arm 0)",
                    "message": "Hello 12345, this is the message for arm 0 of the newcomer condition.",
                },
            ),
            (
                "MarlKarx18",
                {
                    "condition": "experienced",
                    "arm": "arm_1",
                },
                {
                    "subject": "PM Subject Line for MarlKarx18 (Experienced Arm 1)",
                    "message": "Hello MarlKarx18, this is the message for arm 1 of the experienced condition.",
                },
            ),
        ],
    )
    def test_format_intervention_message(
        self, thing_id, metadata_json, want, experiment_controller
    ):
        et = ExperimentThing(
            id=12345,
            thing_id=thing_id,
            experiment_id=experiment_controller.experiment.id,
            object_type=ThingType.USER.value,
            metadata_json=json.dumps(metadata_json),
        )

        got = experiment_controller._format_intervention_message(et)
        assert got == want

    @pytest.mark.parametrize(
        "metadata_json",
        [
            {"condition": "experienced", "arm": "arm_9999"},
            {"condition": "invalid_condition", "arm": "arm_0"},
        ],
    )
    def test_format_intervention_message_raises_error(
        self, metadata_json, experiment_controller
    ):
        et = ExperimentThing(
            id=12345,
            thing_id=23456,
            experiment_id=experiment_controller.experiment.id,
            object_type=ThingType.USER.value,
            metadata_json=json.dumps(metadata_json),
        )

        with pytest.raises(ExperimentConfigurationError):
            experiment_controller._format_intervention_message(et)

    @pytest.mark.parametrize(
        "thing_id,metadata_json,want_error,want_user_query_index,want_user_message_status",
        [
            (
                "ThusSpoke44",
                {
                    "condition": "newcomer",
                    "arm": "arm_1",
                },
                False,
                BannedUserQueryIndex.COMPLETE,
                "sent",
            ),
            (
                "LaLaLatour47",
                {
                    "condition": "experienced",
                    "arm": "arm_2",
                },
                False,
                BannedUserQueryIndex.COMPLETE,
                "sent",
            ),
            (
                "ErrorWhoa99",
                {
                    "condition": "experienced",
                    "arm": "arm_999999",
                },
                True,
                None,
                None,
            ),
            (
                "ErrorErrorOnTheWall11",
                {
                    "condition": "errorconditionhere",
                    "arm": "arm_0",
                },
                True,
                None,
                None,
            ),
        ],
    )
    def test_send_intervention_messages(
        self,
        thing_id,
        metadata_json,
        want_error,
        want_user_query_index,
        want_user_message_status,
        experiment_controller,
    ):
        assert experiment_controller.db_session.query(ExperimentAction).count() == 0
        assert experiment_controller.db_session.query(ExperimentThing).count() == 0

        et = ExperimentThing(
            id=thing_id,
            thing_id=thing_id,
            experiment_id=experiment_controller.experiment.id,
            object_type=ThingType.USER.value,
            metadata_json=json.dumps(metadata_json),
        )
        experiment_controller.db_session.add(et)

        if want_error:
            with pytest.raises(Exception):
                experiment_controller._send_intervention_messages([et])

        else:
            experiment_controller._send_intervention_messages([et])

            ea = (
                experiment_controller.db_session.query(ExperimentAction)
                .filter(ExperimentAction.action_object_id == thing_id)
                .filter(ExperimentAction.action_object_type == ThingType.USER.value)
                .one()
            )

            assert ea is not None
            meta = json.loads(ea.metadata_json)
            assert meta["message_status"] == "sent"

            user = (
                experiment_controller.db_session.query(ExperimentThing)
                .filter(ExperimentThing.thing_id == thing_id)
                .one()
            )
            assert user is not None
            assert user.query_index == want_user_query_index
            meta_u = json.loads(user.metadata_json)
            assert meta_u["message_status"] == "sent"
