import datetime
import os

import pytest
from conftest import DictObject

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

from sqlalchemy import and_

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
    with helpers.with_mock_reddit(modaction_data) as reddit:
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
    return int(datetime.datetime.utcnow().timestamp())


@pytest.fixture
def newcomer_modactions(experiment_controller, modaction_data):
    return experiment_controller._find_first_banstart_candidates(modaction_data)


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


class TestModeratorController:
    def test_archive_mod_action_page(self, db_session, mod_controller):
        assert db_session.query(ModAction).count() == 0

        # Archive the first API result page.
        _, unique_count = mod_controller.archive_mod_action_page()
        assert unique_count == 100

        # Fixtures are split into 100-item pages, and we just loaded one page.
        assert db_session.query(ModAction).count() == 100

    def test_load_all_fixtures(
        self, helpers, db_session, modaction_data, mod_controller
    ):
        # Nothing is loaded yet.
        assert db_session.query(ModAction).count() == 0

        # We have multiple API result pages of fixtures.
        assert len(modaction_data) > 100

        helpers.load_mod_actions(mod_controller)

        # All fixture records were loaded.
        assert db_session.query(ModAction).count() == len(modaction_data)


class TestModactionExperimentController:
    zero_time = datetime.datetime(1970, 1, 1, 0, 0)

    def test_new_modactions_peek(self, helpers, experiment_controller, mod_controller):
        helpers.load_mod_actions(mod_controller)

        ids = []
        with experiment_controller._new_modactions(
            should_save_cursor=False
        ) as modactions:
            # Some mod actions are loaded
            assert len(modactions) > 0
            ids = [m.id for m in modactions]

        # Cursor was not changed
        assert experiment_controller._last_modaction_time() == self.zero_time

        with experiment_controller._new_modactions(
            should_save_cursor=False
        ) as modactions:
            # Some modactions were loaded, again
            assert len(modactions) > 0
            # It's the same list!
            assert [m.id for m in modactions] == ids

    def test_new_modactions_cursor(
        self, helpers, experiment_controller, mod_controller
    ):
        helpers.load_mod_actions(mod_controller)

        # Cursor starts out at zero
        assert experiment_controller._last_modaction_time() == self.zero_time

        ids = []
        with experiment_controller._new_modactions(
            should_save_cursor=True
        ) as modactions:
            # Mod actions are loaded, yay
            assert len(modactions) > 0
            ids = [m.id for m in modactions]

        # Cursor was changed
        assert experiment_controller._last_modaction_time() > self.zero_time

        # Next page
        with experiment_controller._new_modactions(
            should_save_cursor=True
        ) as modactions:
            # More were loaded
            assert len(modactions) > 0
            # It's a new page of results this time
            assert [m.id for m in modactions] != ids


class TestExperimentController:
    def test_find_intervention_targets(
        self, helpers, experiment_controller, mod_controller
    ):
        assert len(experiment_controller._previously_enrolled_user_ids()) == 0

        helpers.load_mod_actions(mod_controller)

        assert len(experiment_controller._previously_enrolled_user_ids()) > 0

    def test_update_experiment(self, experiment_controller):
        experiment_controller.update_experiment()
        # FIXME add integration assertions


class TestModactionPrivateMethods:
    def test_check_condition(self, experiment_controller):
        experiment_controller._check_condition("lurker_threedays")
        experiment_controller._check_condition("lowremoval_sevendays")
        experiment_controller._check_condition("highremoval_fourteendays")
        experiment_controller._check_condition("lurker_thirtydays")
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

    def test_find_first_banstart_candidates(self, modaction_data, experiment_controller):
        # NOTE: not using newcomer_modactions for extra clarity.
        user_modactions = experiment_controller._find_first_banstart_candidates(modaction_data)
        assert len(user_modactions) > 0

    def test_find_second_banover_candidates(self, modaction_data, experiment_controller):
        # NOTE: not using newcomer_modactions for extra clarity.
        user_modactions = experiment_controller._find_second_banover_candidates(modaction_data)
        assert len(user_modactions) > 0

    # update temp ban duration
    @pytest.mark.skip(
        reason="interventions are automatic: skips completed participants"
    )
    @pytest.mark.parametrize(
        "action,details,want_duration,want_query_index,want_type,want_actual_ban_end_time",
        [
            (
                "banuser",
                "999 days",
                999,
                BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                "temporary",
                None,
            ),
            (
                "banuser",
                "permaban",
                None,
                BannedUserQueryIndex.FIRST_BANSTART_IMPOSSIBLE,
                "permanent",
                -1,
            ),
            (
                "unbanuser",
                "whatever",
                None,
                BannedUserQueryIndex.FIRST_BANSTART_IMPOSSIBLE,
                "unbanned",
                "static_now_placeholder",  # placeholder, replaced in test code with value of static_now
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
        want_actual_ban_end_time,
        newcomer_modactions,
        experiment_controller,
        mod_controller,
        static_now,
    ):
        helpers.load_mod_actions(mod_controller)

        original = newcomer_modactions[0]
        update = DictObject(
            {
                **original,
                "action": action,
                "details": details,
                "created_utc": original.created_utc + 1,
            }
        )
        experiment_controller._update_existing_participants(static_now, [update])

        snap = (
            experiment_controller.db_session.query(ExperimentThingSnapshot)
            .filter(
                ExperimentThingSnapshot.experiment_thing_id == original.target_author
            )
            .first()
        )
        assert snap is not None
        assert snap.object_type == ThingType.USER.value
        assert snap.experiment_id == experiment_controller.experiment.id
        meta = json.loads(snap.metadata_json)
        assert meta["ban_type"] == "temporary"

        user = (
            experiment_controller.db_session.query(ExperimentThing)
            .filter(ExperimentThing.thing_id == original.target_author)
            .one()
        )
        assert user is not None
        assert user.query_index == want_query_index

        meta = json.loads(user.metadata_json)
        if want_duration:
            assert meta["ban_duration_days"] == want_duration
        assert meta["ban_type"] == want_type

        if want_actual_ban_end_time == "static_now_placeholder":
            want_actual_ban_end_time = static_now
        assert meta["actual_ban_end_time"] == want_actual_ban_end_time


    @pytest.mark.parametrize(
        "details,want",
        [
            ("3 days", "threedays"),
            ("7 days", "sevendays"),
            ("14 days", "fourteendays"),
            ("30 days", "thirtydays"),
            ("1 day", "unknown"),  # Unknown is default
            ("5 days", "unknown"),  # Irregular numbers of days are errors
        ],
    )
    def test_get_ban_condition(self, details, want, experiment_controller):
        got = experiment_controller._get_ban_condition(
            DictObject({"action": "banuser", "details": details})
        )
        assert got == want

    def test_get_activity_condition(self, static_now, experiment_controller):
        got = experiment_controller._get_activity_condition(DictObject({}), static_now)
        assert got == "lurker"

    def test_enroll_first_banstart_candidates_with_randomized_conditions(
        self, modaction_data, experiment_controller, static_now
    ):
        assert len(experiment_controller._previously_enrolled_user_ids()) == 0

        user_modactions = experiment_controller._find_first_banstart_candidates(modaction_data)
        experiment_controller._enroll_first_banstart_candidates_with_randomized_conditions(static_now, user_modactions)

        assert len(experiment_controller._previously_enrolled_user_ids()) > 1

    def test_assign_second_banover_candidates(
        self, modaction_data, experiment_controller, static_now
    ):
        assert self._count_second_banover_pending(experiment_controller) == 0

        user_modactions = experiment_controller._find_second_banover_candidates(modaction_data)

        assert len(user_modactions) > 1
        experiment_controller._assign_second_banover_candidates(static_now, user_modactions)

        assert self._count_second_banover_pending(experiment_controller) > 1

    def _count_second_banover_pending(self, controller):
        """Helper method to count users with SECOND_BANOVER_PENDING status"""
        return controller.db_session.query(ExperimentThing).filter(
            and_(
                ExperimentThing.experiment_id == controller.experiment.id,
                ExperimentThing.object_type == ThingType.USER.value,
                ExperimentThing.query_index == BannedUserQueryIndex.SECOND_BANOVER_PENDING,
            )
        ).count()

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
        got = experiment_controller._is_tempban(
            DictObject({"action": action, "details": details})
        )
        assert got == want

    @pytest.mark.parametrize(
        "action,want",
        [
            ("unbanuser", True),
            ("banuser", False),
            ("removecomment", False),
        ],
    )
    def test_is_unban(self, action, want, experiment_controller):
        got = experiment_controller._is_unban(
            DictObject({"action": action})
        )
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
        got = experiment_controller._is_tempban_edit(
            DictObject({"action": action, "details": details})
        )
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
    def test_is_valid_tempban_duration(
        self, action, details, want, experiment_controller
    ):
        got = experiment_controller._is_valid_tempban_duration(
            DictObject({"action": action, "details": details})
        )
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
        got = experiment_controller._is_enrolled(
            DictObject({"target_author": username}), choices
        )
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
        got = experiment_controller._is_bot(DictObject({"target_author": username}))
        assert got == want

    @pytest.mark.parametrize(
        "username,want",
        [
            ("innocent_user", False),
            ("[deleted]", True),
        ],
    )
    def test_is_deleted(self, username, want, experiment_controller):
        got = experiment_controller._is_deleted(DictObject({"target_author": username}))
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
                    "actual_ban_end_time": None,
                },
            ),
            ("bogus", {}),
        ],
    )
    def test_parse_temp_ban(self, details, want, experiment_controller):
        got = experiment_controller._parse_temp_ban(
            DictObject(
                {
                    "action": "banuser",
                    "created_utc": 33,
                    "description": "testing",
                    "details": details,
                }
            )
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
        got = experiment_controller._parse_days(
            DictObject({"action": action, "details": details})
        )
        assert got == want

    @pytest.mark.parametrize(
        "test_users,want",
        [
            (
                [{
                    "thing_id": "OlaminaEarthSeedXxX",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                }],
                ["OlaminaEarthSeedXxX"],
            ),            
            (
                [{
                    "thing_id": "marieLVF",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                }],
                [],
            ),            
            (
                [{
                    "thing_id": "edwardsaid35",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                },
                {
                    "thing_id": "user_pending",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                }],
                ["edwardsaid35"],
            ),
        ],
    )
    def test_get_first_banstart_complete_user_ids(
        self, test_users, want, helpers, experiment_controller, mod_controller
    ):

        user_ids = experiment_controller._get_first_banstart_complete_user_ids()
        assert user_ids == []


        for user in test_users:
            et = ExperimentThing(
                id=user["thing_id"],
                thing_id=user["thing_id"],
                experiment_id=experiment_controller.experiment.id,
                object_type=ThingType.USER.value,
                query_index=user["query_index"],
                metadata_json=json.dumps({"ban_type": "temporary"}),
            )
            experiment_controller.db_session.add(et)
        experiment_controller.db_session.commit()

        user_ids = experiment_controller._get_first_banstart_complete_user_ids()
        assert user_ids == want



    @pytest.mark.parametrize(
        "test_users,want",
        [
            (
                [{
                    "thing_id": "user_pending",
                    "query_index": BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                }],
                0,
            ),            
            (
                [{
                    "thing_id": "user_pending",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                }],
                1,
            ),            
            (
                [{
                    "thing_id": "user_complete",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                },
                {
                    "thing_id": "user_pending",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                }],
                1,
            ),
        ],
    )
    def test_get_accounts_needing_first_banstart_interventions(
        self, test_users, want, helpers, experiment_controller, mod_controller
    ):

        users = experiment_controller._get_accounts_needing_first_banstart_interventions()
        assert users == []


        for user in test_users:
            et = ExperimentThing(
                id=user["thing_id"],
                thing_id=user["thing_id"],
                experiment_id=experiment_controller.experiment.id,
                object_type=ThingType.USER.value,
                query_index=user["query_index"],
                metadata_json=json.dumps({"ban_type": "temporary"}),
            )
            experiment_controller.db_session.add(et)
        experiment_controller.db_session.commit()

        users = experiment_controller._get_accounts_needing_first_banstart_interventions()

        assert len(users) == want


    @pytest.mark.parametrize(
        "test_users,want",
        [
            (
                [{
                    "thing_id": "user_pending",
                    "query_index": BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                }],
                1,
            ),            
            (
                [{
                    "thing_id": "user_pending",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                }],
                0,
            ),            
            (
                [{
                    "thing_id": "user_complete",
                    "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                },
                {
                    "thing_id": "user_pending",
                    "query_index": BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                }],
                1,
            ),
        ],
    )
    def test_get_accounts_needing_second_banover_interventions(
        self, test_users, want, helpers, experiment_controller, mod_controller
    ):

        users = experiment_controller._get_accounts_needing_second_banover_interventions()
        assert users == []


        for user in test_users:
            et = ExperimentThing(
                id=user["thing_id"],
                thing_id=user["thing_id"],
                experiment_id=experiment_controller.experiment.id,
                object_type=ThingType.USER.value,
                query_index=user["query_index"],
                metadata_json=json.dumps({"ban_type": "temporary"}),
            )
            experiment_controller.db_session.add(et)
        experiment_controller.db_session.commit()

        users = experiment_controller._get_accounts_needing_second_banover_interventions()

        assert len(users) == want

    @pytest.mark.parametrize(
        "thing_id,metadata_json,want",
        [
            (
                "12345",
                {
                    "condition": "highremoval_threedays",
                    "arm": "arm_0",
                },
                {
                    "subject": "PM Subject Line for 12345 (Threedays Arm 0)",
                    "message": "Hello 12345, this is the message for arm 0 of the highremoval_threedays condition.",
                },
            ),
            (
                "MarlKarx18",
                {
                    "condition": "lurker_fourteendays",
                    "arm": "arm_1",
                },
                {
                    "subject": "PM Subject Line for MarlKarx18 (Fourteendays Arm 1)",
                    "message": "Hello MarlKarx18, this is the message for arm 1 of the lurker_fourteendays condition.",
                },
            ),
        ],
    )
    def test_format_intervention_message__first_banstart(
        self, thing_id, metadata_json, want, experiment_controller
    ):
        et = ExperimentThing(
            id=12345,
            thing_id=thing_id,
            experiment_id=experiment_controller.experiment.id,
            object_type=ThingType.USER.value,
            metadata_json=json.dumps(metadata_json),
        )

        got = experiment_controller._format_intervention_message(et, "first_banstart")
        assert got == want

    @pytest.mark.parametrize(
        "metadata_json",
        [
            {"condition": "lurker_threedays", "arm": "arm_9999"},
            {"condition": "invalid_condition", "arm": "arm_0"},
        ],
    )
    def test_format_intervention_message__first_banstart__raises_error(
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
            experiment_controller._format_intervention_message(et, "first_banstart")



    @pytest.mark.parametrize(
        "thing_id,metadata_json,want",
        [
            (
                "12345",
                {
                },
                {
                    "subject": "PM Subject Line for 12345 (Banover)",
                    "message": "Hello 12345, your ban is over.",
                },
            ),
            (
                "MarlKarx18",
                {
                },
                {
                    "subject": "PM Subject Line for MarlKarx18 (Banover)",
                    "message": "Hello MarlKarx18, your ban is over.",
                },
            ),
        ],
    )
    def test_format_intervention_message__second_banover(
        self, thing_id, metadata_json, want, experiment_controller
    ):
        et = ExperimentThing(
            id=12345,
            thing_id=thing_id,
            experiment_id=experiment_controller.experiment.id,
            object_type=ThingType.USER.value,
            metadata_json=json.dumps(metadata_json),
        )

        got = experiment_controller._format_intervention_message(et, "second_banover")
        assert got == want


    @pytest.mark.parametrize(
        "thing_id,metadata_json,want_error,want_user_query_index,want_user_message_status",
        [
            (
                "ThusSpoke44",
                {
                    "condition": "lowremoval_threedays",
                    "arm": "arm_1",
                },
                False,
                BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                "sent",
            ),
            (
                "LaLaLatour47",
                {
                    "condition": "lurker_fourteendays",
                    "arm": "arm_0",
                },
                False,
                BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                "sent",
            ),
            (
                "ErrorWhoa99",
                {
                    "condition": "lurker_thirtydays",
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
    def test_send_first_banstart_intervention_messages(
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
                experiment_controller._send_first_banstart_intervention_messages([et])

        else:
            experiment_controller._send_first_banstart_intervention_messages([et])

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
