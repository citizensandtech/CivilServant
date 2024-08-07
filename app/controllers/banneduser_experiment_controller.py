from collections import defaultdict
from datetime import datetime
import inspect
import os
import re
import sys

import praw

from app.controllers.modaction_experiment_controller import (
    ModactionExperimentController,
)
from app.models import ExperimentThing, ThingType

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
    3. Send private messages at beginning and end of temporary ban.
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

        self.log.info("Updating the ban state of existing participants")
        self._update_existing_participants(instance.fetched_mod_actions)

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
            # Skip irrelevant mod actions.
            if (
                self._is_enrolled(modaction, previously_enrolled_user_ids)
                or not self._is_tempban(modaction)
                or self._is_bot(modaction)
            ):
                continue

            # NOTE: If there are multiple mod actions for the same user who isn't yet enrolled,
            # we overwrite the previous action with the latest one.
            # This assumes that they are processed in order.
            eligible_newcomers[modaction["target_author"]] = modaction

        return eligible_newcomers

    def _update_existing_participants(self, modactions):
        """Find mod actions that update the state of any current participants.

        Args:
            modactions: A list of mod actions.
        """
        previously_enrolled_user_ids = set(self._previously_enrolled_user_ids())
        updated_users = {}
        for modaction in modactions:
            # Skip mod actions that don't apply to a current participant.
            if not self._is_enrolled(modaction, previously_enrolled_user_ids):
                continue

            # Update the details of the ban, upgrade to a permanent ban, or remove the ban.
            # Assume that actions are in chronological order (last action per user wins).
            if modaction["action"] in ["banuser", "unbanuser"]:
                updated_users[modaction["target_author"]] = modaction

        if not updated_users:
            return

        # Find user records to update.
        users_by_username = {}
        for ut in self.db_session.query(ExperimentThing).filter(
            ExperimentThing.object_type == ThingType.USER.value,
            ExperimentThing.experiment_id == self.experiment.id,
            ExperimentThing.thing_id.in_(updated_users.keys()),
        ):
            users_by_username[ut.thing_id] = ut

        # Apply updates to corresponding `ExperimentThing`s.
        for modaction in updated_users.values():
            # NOTE: This could throw a KeyError, but it shouldn't according to how we build this dict.
            ut = users_by_username[modaction["target_author"]]
            # TODO: Take a snapshot before applying changes.
            if self._is_tempban(modaction):
                # TODO: Temp ban was updated.
                pass
            elif modaction["action"] == "banuser":
                # TODO: Escalated to permaban.
                pass
            elif modaction["action"] == "unbanuser":
                # TODO: User was unbanned.
                pass

    def _get_account_age(self, user_thing):
        user_thing = self._populate_redditor_info(user_thing)
        now_utc = datetime.utcnow().timestamp()
        age_days = (now_utc - user_thing.object_created) / 86400
        if age_days < 7:
            return "weekling"
        else:
            return "oldster"

    def _get_condition(self, user_thing):
        age_bucket = self._get_account_age(user_thing)

        # Combine multiple factors into condition.
        condition = f"{age_bucket}"

        self._check_condition(condition)
        return condition

    def _assign_randomized_conditions(self, newcomer_modactions):
        """Assign randomized conditions to newcomers.
        Log an ExperimentAction with the assignments.
        If ther are no available randomizations, throw an error.
        """
        self.db_session.execute(
            "LOCK TABLES experiments WRITE, experiment_things WRITE"
        )
        try:
            # list of newcomer experiment_things to be added to db
            newcomer_ets = []
            newcomers_without_randomization = 0

            for newcomer in newcomer_modactions:
                condition = self._get_condition(newcomer)

                # Get the next randomization, and ensure that it's valid.
                next_randomization = self.experiment_settings["conditions"][condition][
                    "next_randomization"
                ]
                if next_randomization is not None and next_randomization >= len(
                    self.experiment_settings["conditions"][condition]["randomizations"]
                ):
                    next_randomization = None
                    newcomers_without_randomization += 1
                if next_randomization is None:
                    # If there's no valid randomization for this newcomer, skip it.
                    continue

                # Get the current randomization and increment the experiment's counter.
                randomization = self.experiment_settings["conditions"][condition][
                    "randomizations"
                ][next_randomization]
                self.experiment_settings["conditions"][condition][
                    "next_randomization"
                ] += 1

                et_metadata = {
                    "condition": condition,
                    "randomization": randomization,
                    **self._parse_temp_ban(newcomer),
                }
                et = {
                    "id": uuid.uuid4().hex,
                    "thing_id": newcomer["target_author"],
                    "experiment_id": self.experiment.id,
                    "object_type": ThingType.USER.value,
                    # we don't have account creation info at this stage
                    "object_created": None,
                    "query_index": "Intervention TBD",
                    "metadata_json": json.dumps(et_metadata),
                }
                newcomer_ets.append(et)

            if newcomers_without_randomization > 0:
                self.log.error(
                    f"BanneduserExperimentController Experiment {self.experiment_name} has run out of randomizations from '{condition}' to assign."
                )

            if len(newcomer_ets) > 0:
                self.db_session.insert_retryable(ExperimentThing, newcomer_ets)

                self.experiment.settings_json = json.dumps(self.experiment_settings)
                self.db_session.commit()

            self.log.info(
                f"Assigned randomizations to {len(newcomer_ets)} banned users: [{','.join([x['thing_id'] for x in newcomer_ets])}]"
            )

        except Exception as e:
            self.log.error(
                "Error in BanneduserExperimentController::assign_randomized_conditions"
            )
            return []
        finally:
            self.db_session.execute("UNLOCK TABLES")

    def _is_tempban(self, modaction):
        """Return true if an admin action is a temporary ban.

        For permanent bans, we expect `details` to be "permanent".
        For temporary bans, we expect the number of days, e.g. "7 days".
        """
        return modaction["action"] == "banuser" and "days" in modaction["details"]

    def _is_enrolled(self, modaction, enrolled_user_ids):
        """Return true if the target of an admin action is already enrolled."""
        return modaction["target_author"] in enrolled_user_ids

    def _is_bot(self, modaction):
        """Return true if the user appears to be a bot.

        This is currently a rudimentary approach. Account age is typically a better indicator.
        """
        return modaction["target_author"].endswith("Bot")

    def _parse_temp_ban(self, modaction):
        """Get details about the ban.

        Args:
            modaction: The moderation action for a temporary ban.

        Returns:
            A dict with details about the temporary ban, or None if the action is not a temp ban.
            Note that `ban_start_time` and `ban_end_time` are UNIX timestamps in UTC.

        Example result:
            {
                "ban_duration_days": 30,
                "ban_reason": "Bad behavior",
                "ban_start_time": 1704154715,
                "ban_end_time": 1705277915,
            }
        """
        days = self._parse_days(modaction)
        if days is None:
            return None

        starts_at = int(newcomer["created_utc"])
        ends_at = starts_at + (days * 86400)

        return {
            "ban_duration_days": days,
            "ban_reason": newcomer["description"],
            "ban_start_time": starts_at,
            "ban_end_time": ends_at,
        }

    def _parse_days(self, modaction):
        """Parse the details of a temp ban, returning the ban's duration.

        This is always listed in the form "n days", with n being the number of days.

        Args:
            modaction: The mod action of a temp ban.

        Returns:
            The number of days of the temporary ban, or None.
        """
        if not self._is_tempban(modaction):
            return None

        m = re.search(r"(\d+) days", details, re.IGNORECASE)
        return int(m.group(1)) if m else None
