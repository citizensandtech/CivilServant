from collections import defaultdict
import inspect
import os

import praw

from app.controllers.modaction_experiment_controller import (
    ModactionExperimentController,
)

BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
    "..",
    "..",
)
ENV = os.environ["CS_ENV"]


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
            if self._is_tempban(modaction) and not self._is_enrolled(
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


    def _is_tempban(self, modaction):
        """Return true if an admin action is a temporary ban."""
        return modaction["action"] == "banuser" and "days" in modaction["details"]


    def _is_enrolled(self, modaction, enrolled_user_ids):
        """Return true if the target of an admin action is already enrolled."""
        return modaction["target_author"] not in enrolled_user_ids
