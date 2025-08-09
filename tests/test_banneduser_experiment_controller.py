import datetime
import json
import os
from unittest.mock import patch, Mock

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

    def test_find_first_banstart_candidates(
        self, modaction_data, experiment_controller
    ):
        # NOTE: not using newcomer_modactions for extra clarity.
        user_modactions = experiment_controller._find_first_banstart_candidates(
            modaction_data
        )
        assert len(user_modactions) > 0

    # NOTE: Ideally this test data should be in a fixture
    @pytest.mark.parametrize(
        "test_users,unban_actions,want",
        [
            (
                [
                    {
                        "thing_id": "OlaminaEarthSeedXxX",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    }
                ],
                [
                    {
                        "action": "unbanuser",
                        "target_author": "OlaminaEarthSeedXxX",
                    }
                ],
                ["OlaminaEarthSeedXxX"],
            ),
            (
                [
                    {
                        "thing_id": "marieLVF",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    }
                ],
                [
                    {
                        "action": "unbanuser",
                        "target_author": "marieLVF",
                    }
                ],
                [],
            ),
            (
                [
                    {
                        "thing_id": "cgj75",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    }
                ],
                [
                    {
                        "action": "banuser",
                        "target_author": "cgj75",
                    }
                ],
                [],
            ),
            (
                [
                    {
                        "thing_id": "yalomyalom31",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    }
                ],
                [
                    {
                        "action": "unbanuser",
                        "target_author": "yalomyalom31",
                    },
                    {
                        "action": "unbanuser",
                        "target_author": "yalomyalom31",  # duplicate unban
                    },
                ],
                ["yalomyalom31"],
            ),
            (
                [
                    {
                        "thing_id": "rogers123",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    }
                ],
                [
                    {
                        "action": "unbanuser",
                        "target_author": "rogers123",
                    },
                    {
                        "action": "banuser",
                        "target_author": "rogers123",  # ban after unban
                        "details": "3 days",
                    },
                ],
                [],
            ),
            (
                [
                    {
                        "thing_id": "edwardsaid35",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    },
                    {
                        "thing_id": "abc123",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    },
                ],
                [
                    {
                        "action": "unbanuser",
                        "target_author": "edwardsaid35",
                    },
                    {
                        "action": "unbanuser",
                        "target_author": "user_pending",
                    },
                ],
                ["edwardsaid35"],
            ),
        ],
    )
    def test_find_second_banover_candidates(
        self, test_users, unban_actions, want, modaction_data, experiment_controller
    ):
        user_modactions = experiment_controller._find_second_banover_candidates(
            modaction_data
        )
        assert len(user_modactions) == 0

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

        unban_actions = [DictObject(action) for action in unban_actions]

        user_modactions = experiment_controller._find_second_banover_candidates(
            unban_actions
        )
        found_users = [m.target_author for m in user_modactions]
        assert found_users == want

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

        user_modactions = experiment_controller._find_first_banstart_candidates(
            modaction_data
        )
        experiment_controller._enroll_first_banstart_candidates_with_randomized_conditions(
            static_now, user_modactions
        )

        assert len(experiment_controller._previously_enrolled_user_ids()) > 1

    @pytest.mark.parametrize(
        "test_users,want",
        [
            (
                [
                    {
                        "target_author": "OlaminaEarthSeedXxX",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    }
                ],
                1,
            ),
            (
                [
                    {
                        "target_author": "marieLVF",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    }
                ],
                0,
            ),
            (
                [
                    {
                        "target_author": "edwardsaid35",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    },
                    {
                        "target_author": "user_pending",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    },
                ],
                1,
            ),
        ],
    )
    def test_assign_second_banover_candidates(
        self, test_users, want, experiment_controller, static_now
    ):
        assert self._count_second_banover_pending(experiment_controller) == 0

        # first, insert experiment things into database
        for user in test_users:
            et = ExperimentThing(
                id=user["target_author"],
                thing_id=user["target_author"],
                experiment_id=experiment_controller.experiment.id,
                object_type=ThingType.USER.value,
                query_index=user["query_index"],
                metadata_json=json.dumps({"ban_type": "temporary"}),
            )
            experiment_controller.db_session.add(et)
        experiment_controller.db_session.commit()

        test_user_objects = [DictObject(user) for user in test_users]

        experiment_controller._assign_second_banover_candidates(
            static_now, test_user_objects
        )

        assert self._count_second_banover_pending(experiment_controller) == want

    def _count_second_banover_pending(self, controller):
        """Helper method to count users with SECOND_BANOVER_PENDING status"""
        return (
            controller.db_session.query(ExperimentThing)
            .filter(
                and_(
                    ExperimentThing.experiment_id == controller.experiment.id,
                    ExperimentThing.object_type == ThingType.USER.value,
                    ExperimentThing.query_index
                    == BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                )
            )
            .count()
        )

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
        got = experiment_controller._is_unban(DictObject({"action": action}))
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

    # NOTE: Ideally this test data should be in a fixture
    @pytest.mark.parametrize(
        "test_users,want",
        [
            (
                [
                    {
                        "thing_id": "OlaminaEarthSeedXxX",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    }
                ],
                ["OlaminaEarthSeedXxX"],
            ),
            (
                [
                    {
                        "thing_id": "marieLVF",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    }
                ],
                [],
            ),
            (
                [
                    {
                        "thing_id": "edwardsaid35",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    },
                    {
                        "thing_id": "user_pending",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    },
                ],
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

    # NOTE: Ideally this test data should be in a fixture
    @pytest.mark.parametrize(
        "test_users,want",
        [
            (
                [
                    {
                        "thing_id": "user_pending",
                        "query_index": BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                    }
                ],
                0,
            ),
            (
                [
                    {
                        "thing_id": "user_pending",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    }
                ],
                1,
            ),
            (
                [
                    {
                        "thing_id": "user_complete",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    },
                    {
                        "thing_id": "user_pending",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    },
                ],
                1,
            ),
        ],
    )
    def test_get_accounts_needing_first_banstart_interventions(
        self, test_users, want, helpers, experiment_controller, mod_controller
    ):
        users = (
            experiment_controller._get_accounts_needing_first_banstart_interventions()
        )
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

        users = (
            experiment_controller._get_accounts_needing_first_banstart_interventions()
        )

        assert len(users) == want

    # NOTE: Ideally this test data should be in a fixture
    @pytest.mark.parametrize(
        "test_users,want",
        [
            (
                [
                    {
                        "thing_id": "user_pending",
                        "query_index": BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                    }
                ],
                1,
            ),
            (
                [
                    {
                        "thing_id": "user_pending",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                    }
                ],
                0,
            ),
            (
                [
                    {
                        "thing_id": "user_complete",
                        "query_index": BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                    },
                    {
                        "thing_id": "user_pending",
                        "query_index": BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                    },
                ],
                1,
            ),
        ],
    )
    def test_get_accounts_needing_second_banover_interventions(
        self, test_users, want, helpers, experiment_controller, mod_controller
    ):
        users = (
            experiment_controller._get_accounts_needing_second_banover_interventions()
        )
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

        users = (
            experiment_controller._get_accounts_needing_second_banover_interventions()
        )

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
                {},
                {
                    "subject": "PM Subject Line for 12345 (Banover)",
                    "message": "Hello 12345, your ban is over.",
                },
            ),
            (
                "MarlKarx18",
                {},
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

    # NOTE: Ideally this test data should be in a fixture
    @pytest.mark.parametrize(
        "thing_id,metadata_json,should_raise_error,expected_query_index,expected_message_status",
        [
            (
                "ThusSpoke44",
                {
                    "condition": "lowremoval_threedays",
                    "arm": "arm_1",
                },
                False,
                BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                "first_banstart_sent",
            ),
            (
                "LaLaLatour47",
                {
                    "condition": "lurker_fourteendays",
                    "arm": "arm_0",
                },
                False,
                BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                "first_banstart_sent",
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
        should_raise_error,
        expected_query_index,
        expected_message_status,
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

        if should_raise_error:
            with pytest.raises(ExperimentConfigurationError):
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
            assert meta["message_status"] == expected_message_status

            user = (
                experiment_controller.db_session.query(ExperimentThing)
                .filter(ExperimentThing.thing_id == thing_id)
                .one()
            )
            assert user is not None
            assert user.query_index == expected_query_index
            meta_u = json.loads(user.metadata_json)
            assert meta_u["message_status"] == expected_message_status

    @pytest.mark.parametrize(
        "thing_id,metadata_json,expected_query_index,expected_message_status",
        [
            (
                "ThusSpoke44",
                {"condition": "lowremoval_threedays", "arm": "arm_1"},
                BannedUserQueryIndex.SECOND_BANOVER_COMPLETE,
                "second_banover_sent",
            ),
            (
                "LaLaLatour47",
                {"condition": "lurker_fourteendays", "arm": "arm_0"},
                BannedUserQueryIndex.SECOND_BANOVER_COMPLETE,
                "second_banover_sent",
            ),
        ],
    )
    def test_send_second_banover_intervention_messages(
        self,
        thing_id,
        metadata_json,
        expected_query_index,
        expected_message_status,
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

        experiment_controller._send_second_banover_intervention_messages([et])

        ea = (
            experiment_controller.db_session.query(ExperimentAction)
            .filter(ExperimentAction.action_object_id == thing_id)
            .filter(ExperimentAction.action_object_type == ThingType.USER.value)
            .one()
        )

        assert ea is not None
        meta = json.loads(ea.metadata_json)
        assert meta["message_status"] == expected_message_status

        user = (
            experiment_controller.db_session.query(ExperimentThing)
            .filter(ExperimentThing.thing_id == thing_id)
            .one()
        )
        assert user is not None
        assert user.query_index == expected_query_index
        meta_u = json.loads(user.metadata_json)
        assert meta_u["message_status"] == expected_message_status


class TestBanneduserMessageSending:
    @pytest.fixture
    def mock_messaging_controller(self):
        with patch(
            "app.controllers.banneduser_experiment_controller.MessagingController"
        ) as mock_mc_class:
            mock_mc_instance = Mock()
            mock_mc_class.return_value = mock_mc_instance
            yield mock_mc_instance

    @pytest.fixture
    def experiment_thing_factory(self, db_session):
        def _create_experiment_thing(
            experiment_controller, thing_id, metadata=None, arm="arm_1"
        ):
            if metadata is None:
                metadata = {
                    "condition": "lurker_fourteendays",
                    "arm": arm,
                    "randomization_time": "2023-01-01T00:00:00Z",
                }
            et = ExperimentThing(
                id=thing_id,
                thing_id=thing_id,
                experiment_id=experiment_controller.experiment.id,
                object_type=ThingType.USER.value,
                metadata_json=json.dumps(metadata),
            )
            db_session.add(et)
            db_session.commit()
            return et

        return _create_experiment_thing

    @pytest.fixture
    def db_queries(self, db_session):
        class DBQueries:
            def get_experiment_thing(self, thing_id):
                return (
                    db_session.query(ExperimentThing)
                    .filter(ExperimentThing.thing_id == thing_id)
                    .one()
                )

            def get_experiment_action(self, thing_id):
                return (
                    db_session.query(ExperimentAction)
                    .filter(ExperimentAction.action_object_id == thing_id)
                    .one()
                )

            def count_experiment_actions(self, thing_id):
                return (
                    db_session.query(ExperimentAction)
                    .filter(ExperimentAction.action_object_id == thing_id)
                    .count()
                )

        return DBQueries()

    @pytest.mark.parametrize(
        "message_type,thing_id,expected_query_index,expected_message_status",
        [
            (
                "first_banstart",
                "test_user",
                BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
                "first_banstart_sent",
            ),
            (
                "second_banover",
                "test_user2",
                BannedUserQueryIndex.SECOND_BANOVER_COMPLETE,
                "second_banover_sent",
            ),
        ],
    )
    def test_send_intervention_messages_success(
        self,
        message_type,
        thing_id,
        expected_query_index,
        expected_message_status,
        experiment_controller,
        mock_messaging_controller,
        experiment_thing_factory,
        db_queries,
    ):
        mock_messaging_controller.send_messages.return_value = {
            thing_id: {"errors": []}
        }
        et = experiment_thing_factory(experiment_controller, thing_id)

        if message_type == "first_banstart":
            result = experiment_controller._send_first_banstart_intervention_messages(
                [et]
            )
        else:
            result = experiment_controller._send_second_banover_intervention_messages(
                [et]
            )

        assert len(result) == 1, f"expected 1 message log entry, got {len(result)}"
        assert thing_id in result, f"message log missing entry for user {thing_id}"

        updated_et = db_queries.get_experiment_thing(thing_id)
        assert updated_et.query_index == expected_query_index, (
            f"experiment thing query_index not updated: expected {expected_query_index}, got {updated_et.query_index}"
        )
        metadata = json.loads(updated_et.metadata_json)
        assert metadata["message_status"] == expected_message_status, (
            f"experiment thing message_status incorrect: expected {expected_message_status}, got {metadata.get('message_status')}"
        )

        # Verify experiment action was created
        db_queries.get_experiment_action(thing_id)

    @pytest.mark.parametrize(
        "message_type,error_type,attempts_to_run,expects_impossible",
        [
            # Test single attempt for various error types (should record retry, not set impossible)
            ("first_banstart", "invalid username", 1, False),
            ("second_banover", "invalid username", 1, False),
            ("first_banstart", "captcha required", 1, False),
            ("second_banover", "captcha required", 1, False),
            ("first_banstart", "rate limit exceeded", 1, False),
            ("second_banover", "server error", 1, False),
            # Test two attempts (should increment retry counter, still not set impossible)
            ("first_banstart", "captcha required", 2, False),
            ("second_banover", "captcha required", 2, False),
            # Test three attempts for various error types (should set impossible status)
            ("first_banstart", "invalid username", 3, True),
            ("second_banover", "invalid username", 3, True),
            ("first_banstart", "captcha required", 3, True),
            ("second_banover", "captcha required", 3, True),
            ("first_banstart", "rate limit exceeded", 3, True),
            ("second_banover", "server error", 3, True),
        ],
    )
    def test_send_intervention_messages_with_errors(
        self,
        message_type,
        error_type,
        attempts_to_run,
        expects_impossible,
        experiment_controller,
        mock_messaging_controller,
        experiment_thing_factory,
    ):
        """Test message sending with various error types and attempt patterns."""
        thing_id = f"{error_type.replace(' ', '_')}_user_{attempts_to_run}_attempts"
        mock_messaging_controller.send_messages.return_value = {
            thing_id: {"errors": [{"username": thing_id, "error": error_type}]}
        }

        et = experiment_thing_factory(experiment_controller, thing_id)
        original_query_index = et.query_index

        send_method = (
            experiment_controller._send_first_banstart_intervention_messages
            if message_type == "first_banstart"
            else experiment_controller._send_second_banover_intervention_messages
        )

        # Execute the specified number of attempts
        for attempt in range(attempts_to_run):
            send_method([et])
            et = (
                experiment_controller.db_session.query(ExperimentThing)
                .filter(ExperimentThing.thing_id == thing_id)
                .one()
            )

        # Verify query_index behavior
        if expects_impossible:
            expected_impossible_index = (
                BannedUserQueryIndex.FIRST_BANSTART_IMPOSSIBLE
                if message_type == "first_banstart"
                else BannedUserQueryIndex.SECOND_BANOVER_IMPOSSIBLE
            )
            assert et.query_index == expected_impossible_index, (
                f"Should set impossible status after {attempts_to_run} attempts with '{error_type}'"
            )

            expected_message_status = (
                "first_banstart_failed"
                if message_type == "first_banstart"
                else "second_banover_failed"
            )
            metadata = json.loads(et.metadata_json)
            assert metadata["message_status"] == expected_message_status, (
                f"Should set expected message status: expected {expected_message_status}, got {metadata.get('message_status')}"
            )
        else:
            assert et.query_index == original_query_index, (
                f"Should not change query_index on first attempt with '{error_type}'"
            )

        # Verify retry count and error details
        metadata = json.loads(et.metadata_json)
        assert metadata["message_retry_count"] == attempts_to_run, (
            f"Should have retry count = {attempts_to_run} for '{error_type}'"
        )
        assert metadata["last_message_error"] == error_type, (
            f"Should store error details for '{error_type}'"
        )

        # Verify ExperimentActions created
        ea_count = (
            experiment_controller.db_session.query(ExperimentAction)
            .filter(ExperimentAction.action_object_id == thing_id)
            .count()
        )
        assert ea_count == attempts_to_run, (
            f"Should create {attempts_to_run} ExperimentAction(s) for '{error_type}'"
        )

    def test_send_intervention_messages_mixed_results(
        self, experiment_controller, mock_messaging_controller, experiment_thing_factory
    ):
        mock_messaging_controller.send_messages.return_value = {
            "success_user": {"errors": []},
            "invalid_user": {
                "errors": [{"username": "invalid_user", "error": "invalid username"}]
            },
            "captcha_user": {
                "errors": [{"username": "captcha_user", "error": "captcha required"}]
            },
        }

        success_et = experiment_thing_factory(experiment_controller, "success_user")
        invalid_et = experiment_thing_factory(experiment_controller, "invalid_user")
        captcha_et = experiment_thing_factory(experiment_controller, "captcha_user")

        experiment_things = [success_et, invalid_et, captcha_et]
        experiment_controller._send_first_banstart_intervention_messages(
            experiment_things
        )

        success_et_updated = (
            experiment_controller.db_session.query(ExperimentThing)
            .filter(ExperimentThing.thing_id == "success_user")
            .one()
        )
        assert (
            success_et_updated.query_index
            == BannedUserQueryIndex.FIRST_BANSTART_COMPLETE
        )
        success_meta = json.loads(success_et_updated.metadata_json)
        assert success_meta["message_status"] == "first_banstart_sent"

        invalid_et_updated = (
            experiment_controller.db_session.query(ExperimentThing)
            .filter(ExperimentThing.thing_id == "invalid_user")
            .one()
        )
        # Invalid username should not set impossible on first attempt (requires 3 attempts)
        assert invalid_et_updated.query_index == invalid_et.query_index, (
            "Invalid username should not change query_index on first attempt"
        )
        invalid_meta = json.loads(invalid_et_updated.metadata_json)
        assert invalid_meta["message_retry_count"] == 1, (
            "Should record retry attempt for invalid username"
        )

        captcha_et_updated = (
            experiment_controller.db_session.query(ExperimentThing)
            .filter(ExperimentThing.thing_id == "captcha_user")
            .one()
        )
        assert captcha_et_updated.query_index == captcha_et.query_index, (
            "Captcha error should not change query_index on first attempt"
        )
        captcha_meta = json.loads(captcha_et_updated.metadata_json)
        assert captcha_meta["message_retry_count"] == 1, (
            "Should record retry attempt for captcha error"
        )

        success_ea = (
            experiment_controller.db_session.query(ExperimentAction)
            .filter(ExperimentAction.action_object_id == "success_user")
            .one()
        )
        assert success_ea is not None, "Should create action for successful message"

        invalid_ea = (
            experiment_controller.db_session.query(ExperimentAction)
            .filter(ExperimentAction.action_object_id == "invalid_user")
            .one()
        )
        assert invalid_ea is not None, "Should create action for invalid username retry"

        captcha_ea = (
            experiment_controller.db_session.query(ExperimentAction)
            .filter(ExperimentAction.action_object_id == "captcha_user")
            .one()
        )
        assert captcha_ea is not None, "Should create action for captcha error retry"

    @pytest.mark.parametrize(
        "message_type,attempt_count",
        [
            ("first_banstart", 1),
            ("first_banstart", 2),
            ("first_banstart", 3),
            ("second_banover", 1),
            ("second_banover", 2),
            ("second_banover", 3),
        ],
    )
    def test_message_error_retry_progression(
        self,
        message_type,
        attempt_count,
        experiment_controller,
        mock_messaging_controller,
        experiment_thing_factory,
    ):
        """Test message error retry progression: 1st/2nd attempts record retries, 3rd sets impossible status."""
        error_type = "temporary error"
        thing_id = f"user_attempt_{attempt_count}_{message_type}"
        mock_messaging_controller.send_messages.return_value = {
            thing_id: {"errors": [{"username": thing_id, "error": error_type}]}
        }

        et = experiment_thing_factory(experiment_controller, thing_id)
        original_query_index = et.query_index

        send_method = (
            experiment_controller._send_first_banstart_intervention_messages
            if message_type == "first_banstart"
            else experiment_controller._send_second_banover_intervention_messages
        )

        # Execute the specified number of attempts
        for attempt in range(attempt_count):
            send_method([et])
            et = (
                experiment_controller.db_session.query(ExperimentThing)
                .filter(ExperimentThing.thing_id == thing_id)
                .one()
            )

        # Verify behavior based on attempt count
        if attempt_count < 3:
            # First two attempts should NOT set impossible status
            assert et.query_index == original_query_index, (
                f"Attempt {attempt_count} should not set impossible status for '{error_type}'"
            )
        else:
            # Third attempt should set impossible status
            expected_impossible = (
                BannedUserQueryIndex.FIRST_BANSTART_IMPOSSIBLE
                if message_type == "first_banstart"
                else BannedUserQueryIndex.SECOND_BANOVER_IMPOSSIBLE
            )
            assert et.query_index == expected_impossible, (
                f"Third attempt should set impossible status for '{error_type}'"
            )

        # Verify retry count and error details
        metadata = json.loads(et.metadata_json)
        assert metadata.get("message_retry_count") == attempt_count, (
            f"Should have retry count = {attempt_count} for '{error_type}'"
        )
        assert metadata.get("last_message_error") == error_type, (
            f"Should store error details for '{error_type}'"
        )

        # Verify ExperimentActions created
        ea_count = (
            experiment_controller.db_session.query(ExperimentAction)
            .filter(ExperimentAction.action_object_id == thing_id)
            .count()
        )
        assert ea_count == attempt_count, (
            f"Should create {attempt_count} ExperimentAction(s) for '{error_type}'"
        )

    def test_send_first_banstart_intervention_message(
        self, experiment_controller, mock_messaging_controller, experiment_thing_factory
    ):
        # Test successful first banstart intervention message sending
        mock_messaging_controller.send_messages.return_value = {
            "test_user": {"errors": []}
        }

        thing_id = "test_user"
        et = experiment_thing_factory(
            experiment_controller,
            thing_id,
            metadata={
                "condition": "lurker_fourteendays",
                "arm": "arm_0",
                "randomization_time": "2023-01-01T00:00:00Z",
            },
        )

        experiment_controller._send_first_banstart_intervention_messages([et])

        updated_et = (
            experiment_controller.db_session.query(ExperimentThing)
            .filter(ExperimentThing.thing_id == thing_id)
            .one()
        )
        assert updated_et.query_index == BannedUserQueryIndex.FIRST_BANSTART_COMPLETE, (
            f"query_index not updated correctly: expected {BannedUserQueryIndex.FIRST_BANSTART_COMPLETE}, got {updated_et.query_index}"
        )

        metadata = json.loads(updated_et.metadata_json)
        assert metadata["message_status"] == "first_banstart_sent", (
            f"message_status not updated: expected 'first_banstart_sent', got {metadata.get('message_status')}"
        )

        # Verify experiment action was created
        experiment_controller.db_session.query(ExperimentAction).filter(
            ExperimentAction.action_object_id == thing_id
        ).one()
