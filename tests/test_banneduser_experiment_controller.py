from dataclasses import dataclass
import os
import glob
import simplejson as json
import praw
from collections import defaultdict
from unittest.mock import MagicMock, Mock, patch
import uuid

import pytest
import simplejson as json

# XXX: must come before app imports
ENV = os.environ["CS_ENV"] = "test"

from app.controllers.banneduser_experiment_controller import (
    BanneduserExperimentController,
)
from app.controllers.moderator_controller import ModeratorController
import app.cs_logger
from utils.common import DbEngine
from app.models import *

import traceback
import logging

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
        
        @dataclass
        class MockRedditor:
            created_utc = 123
        r.redditor = MagicMock(return_value=MockRedditor())

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



#### TEST BanneduserExperimentController::get_accounts_needing_interventions
def test_interventions(experiment_controller, reddit_return_value):

    ## SET UP FIXTURES AND INITIALIZE DATABASE


    m = Mock()
    # Fixture data is broken up like this to allow testing of API 'pagination'
    """
    m.side_effect = "yo"
    reddit_return_value.redditorr = m
    logging.info(reddit_return_value.redditorr)
    """


    modaction_fixtures = []
    for filename in sorted(glob.glob("{script_dir}/fixture_data/modactions_20240703/mod_actions_1*".format(script_dir=TEST_DIR))):
        f = open(filename, "r")
        modaction_fixtures += json.loads(f.read())
        f.close()
    
    ## IN THIS CASE, WE ARE GENERATING TARGET_AUTHOR IDs
    ## LEST A BUG ACCIDENTALLY SEND PEOPLE COMMENTS
    ## WHILE WE ARE UNIT TESTING. reddit has a 20 character limit
    ## so any uuid4 will be an invalid username on reddit
    fetched_mod_actions = []
    for modaction in modaction_fixtures:
        author = uuid.uuid4().hex
        modaction['target_author']  = author
        fetched_mod_actions.append(modaction)



    # FIXME: SHOULD WE DUPLICATE LOGIC OF enroll_new_participants here?



    try:
        eligible_newcomers = experiment_controller._find_eligible_newcomers(fetched_mod_actions)
    except Exception as e:
        logging.info("Error in BanneduserExperimentController::_find_eligible_newcomers: %s", str(e))
        logging.info("Traceback: %s", traceback.format_exc())
        #FIXME how should an exception be logged?

    # TODO - craft assert based on data?
    #assert len(eligible_newcomers) == 10


    try:
        experiment_controller._assign_randomized_conditions(eligible_newcomers)
    except Exception as e:
        logging.info("Error in BanneduserExperimentController::_assign_randomized_conditions: %s", str(e))
        logging.info("Traceback: %s", traceback.format_exc())
        #FIXME how should an exception be logged?
        #logger.exception("Error in BanneduserExperimentController::assign_randomized_conditions")



    # FIXME
    """
    try:
        experiment_controller._update_existing_participants(fetched_mod_actions)
    except Exception as e:
        logging.info("Error in BanneduserExperimentController::_update_existing_participants: %s", str(e))
        logging.info("Traceback: %s", traceback.format_exc())
        #FIXME how should an exception be logged?
        #logger.exception("Error in BanneduserExperimentController::assign_randomized_conditions")
    """   
        

    ## TEST the result from get accounts needing intervention
    accounts_needing_intervention = experiment_controller._get_accounts_needing_interventions()

   
    # TODO - craft assert based on data?
    #assert len(accounts_needing_intervention) == len(newcomer_modactions)


    # FIXME: Not sure why we need to do an assert here?
    fetched_modactioned_accounts = [x['target_author'] for x in fetched_mod_actions]
    for account in accounts_needing_intervention:

        message = experiment_controller._format_intervention_message(account)
        #logging.info(message)

        assert account.thing_id in fetched_modactioned_accounts




    ## TEST the formatting of messages
    # first case: where the arm is arm_1 as specified in the randomizations csv
    arm_1_experiment_thing = [x for x in accounts_needing_intervention if json.loads(x.metadata_json)['arm']=="arm_1"][0]
    message_output = experiment_controller._format_intervention_message(arm_1_experiment_thing)
    # TODO craft assert based on data 
    assert message_output['message'].find("Hello" ) > -1
    # second case: where the arm is null experiment_controllerause it's the control group
    # in that case, the message output should be None


    # TODO craft additional asserts based on specifics of arm behavior



    ## TEST the result from sending messages
    m = Mock()
    message_return_vals = []

    """
    ## SET UP accounts_to_test return values from message sending
    ## the final account in the set will be an invalid username error
    #for i in range(accounts_to_test-1):
    #    message_return_vals.append({"errors":[]})
    message_return_vals.append({"errors":[]})
    message_return_vals.append({"errors":[{"username":fetched_modactioned_accounts[accounts_to_test -2],
                                "error": "nondescript error"}]})    
    message_return_vals.append(
        {"errors":[{"username":fetched_modactioned_accounts[accounts_to_test-1], 
        "error":"invalid username"}]})

    m.side_effect = message_return_vals
    r.send_message = m
    patch('praw.')

    experiment_things = accounts_needing_intervention[0:accounts_to_test]
    
    message_results = mec.send_messages(experiment_things)
    
    ## assertions for ExperimentAction objects
    experiment_actions = db_session.query(ExperimentAction).all()
    assert len(experiment_actions) == 3
    ea = json.loads(experiment_actions[0].metadata_json)
    assert(ea['survey_status']=="TBD")
    assert(ea['message_status']=="sent")
    ea = json.loads(experiment_actions[1].metadata_json)
    assert(ea['survey_status']=="TBD")
    assert(ea['message_status']=="sent")
    ea = json.loads(experiment_actions[2].metadata_json)
    assert(ea['survey_status']=="nonexistent")
    assert(ea['message_status']=="nonexistent")

    ## assertions for ExperimentThing objects
    outcome_bundles = [{"query_index": "Intervention Complete",
                    "message_status": "sent",
                    "survey_status": "TBD",
                    "observed_count":0},
                    {"query_index":"Intervention TBD",
                     "message_status":"TBD",
                     "survey_status":"TBD",
                     "observed_count":0},
                    {"query_index":"Intervention Impossible",
                     "message_status":"nonexistent",
     """

