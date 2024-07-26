import praw
import inspect, os, sys, uuid  # set the BASE_DIR
import simplejson as json
import datetime, yaml, time, csv
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
import sqlalchemy
from dateutil import parser
from utils.common import *
from app.models import Base, SubredditPage, Subreddit, Post, ModAction, PrawKey, Comment
from app.models import (
    Experiment,
    ExperimentThing,
    ExperimentAction,
    ExperimentThingSnapshot,
)
from app.models import EventHook
from sqlalchemy import and_, or_, not_, asc, desc
from app.controllers.messaging_controller import MessagingController
from app.controllers.experiment_controller import *
from collections import defaultdict

BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
    "..",
    "..",
)
ENV = os.environ["CS_ENV"]


class ModactionExperimentController(ExperimentController):
    """
    Mod Action experiment controller.

    A superclass with methods that are useful for by any experiment on Reddit moderation actions.

    Callback methods must accept these 2 arguments:
        self: An instance of callee class
        instance: An instance of caller class
    """

    def __init__(
        self, experiment_name, db_session, r, log, required_keys=["event_hooks"]
    ):

        super().__init__(experiment_name, db_session, r, log, required_keys)

    def enroll_new_participants(self, instance):
        """This is essentially an abstract method."""
        if instance.fetched_subreddit_id != self.experiment_settings["subreddit_id"]:
            return
        self.log.info(
            f"Successfully Ran Event Hook to ModactionExperimentController::enroll_new_participants. Caller: {instance}"
        )

    def _get_condition(self):
        """Get the condition name to use in this experiment."""
        if "main" not in self.experiment_settings["conditions"].keys():
            self.log.error("Condition 'main' missing from configuration file.")
            raise Exception("Condition 'main' missing from configuration file")
        return "main"

    def _previously_enrolled_user_ids(self):
        """Get user IDs that are already enrolled in this study.

        Returns:
            A list of user IDs.
        """
        user_ids = self.db_session.query(ExperimentThing.thing_id).filter(
            and_(
                ExperimentThing.experiment_id == self.experiment.id,
                ExperimentThing.object_type == ThingType.USER.value,
            )
        )
        return user_ids.all()


class BanneduserExperimentController(ModactionExperimentController):
    """Banned user experiment controller.


    This experiment controller should:
    1. Observe modactions to identify and enroll new participants.
    2. Randomly assign these users to receive different types of private messages.
    3. TBD
    """

    def __init__(
        self, experiment_name, db_session, r, log, required_keys=["event_hooks"]
    ):
        super().__init__(experiment_name, db_session, r, log, required_keys)

    def _find_eligible_newcomers(self, modactions):
        """Filter a list of mod actions to find newcomers to the experiment.
        Starting with a list of arbitrary mod actions, select mod actions that:
        - are temporary bans, and
        - are not for users already in the study.

        Args:
            modactions: A list of mod actions.

        Returns:
            A dict of relevant mod actions, indexed by the new user's ID.
        """
        previously_enrolled_user_ids = set(self._previously_enrolled_user_ids())
        eligible_newcomers = {}
        for modaction in modactions:
            if _is_tempban(modaction) and not _is_enrolled(
                modaction, previously_enrolled_user_ids
            ):
                eligible_newcomers[modaction["target_author"]] = modaction
                # TODO: handle logic if the same user has multiple ban events
        return eligible_newcomers

    def _assign_randomized_conditions(self, newcomer_modactions):
        """Assign randomized conditions to newcomers.
        Log an ExperimentAction with the assignments.
        If ther are no available randomizations, throw an error.
        """
        condition = self._get_condition()

        newcomer_ids = newcomer_modactions.keys()

        self.log.info(newcomer_ids)

        self.db_session.execute(
            "LOCK TABLES experiments WRITE, experiment_things WRITE"
        )
        try:
            # list of newcomer experiment_things to be added to db
            newcomer_ets = []
            newcomers_without_randomization = 0
            next_randomization = self.experiment_settings['conditions'][condition]['next_randomization']


            self.log.info(self.experiment_settings['conditions'][condition]['randomizations'])

            for newcomer in newcomer_modactions:
                et_metadata = {}


                self.log.info(newcomer)

                # WRITE STUFF

        except(Exception) as e:
           self.log.error("Error in BanneduserExperimentController::assign_randomized_conditions", extra=sys.exc_info()[0])
           return []
        finally:
           self.db_session.execute("UNLOCK TABLES")

    def enroll_new_participants(self, instance):
        """Enroll new participants in the experiment.

        This is a callback that will be invoked declaratively.
        """
        if instance.fetched_subreddit_id != self.experiment_settings["subreddit_id"]:
            return

        self.log.info(
            f"Experiment {self.experiment.name}: scanning modactions in subreddit {self.experiment_settings['subreddit_id']} to look for temporary bans"
        )
        eligible_newcomers = self._find_eligible_newcomers(instance.fetched_mod_actions)

        self.log.info("Assigning randomized conditions to eligible newcomers")
        self._assign_randomized_conditions(eligible_newcomers)

        self.log.info(
            f"Successfully Ran Event Hook to BanneduserExperimentController::enroll_new_participants. Caller: {instance}"
        )


def _is_tempban(modaction):
    """Return true if an admin action is a temporary ban."""
    return modaction["action"] == "banuser" and "days" in modaction["details"]


def _is_enrolled(modaction, enrolled_user_ids):
    """Return true if the target of an admin action is already enrolled."""
    return modaction["target_author"] not in enrolled_user_ids
