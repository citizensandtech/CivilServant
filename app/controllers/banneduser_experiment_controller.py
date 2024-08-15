from datetime import datetime, timedelta
from enum import Enum
import inspect
import json
import os
import re
import sys

import uuid
from sqlalchemy import and_

from app.controllers.experiment_controller import ExperimentConfigurationError
from app.controllers.modaction_experiment_controller import (
    ModactionExperimentController,
)
from app.controllers.messaging_controller import (
    MessagingController,
)
from app.models import (
    ExperimentAction,
    ExperimentThing,
    ExperimentThingSnapshot,
    ThingType,
)

BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
    "..",
    "..",
)
ENV = os.environ["CS_ENV"]


class BannedUserQueryIndex(str, Enum):
    """Possible states of a banned user's query_index."""

    COMPLETE = "Intervention Complete"
    IMPOSSIBLE = "Intervention Impossible"
    TBD = "Intervention TBD"


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

        This is a callback that will be invoked declaratively. This is called by ModeratorController while running archive_mod_action_page, as noted in the experiment config YAML File.
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
            A list of relevant mod actions
        """
        previously_enrolled_user_ids = set(self._previously_enrolled_user_ids())
        eligible_newcomers = []
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
            eligible_newcomers.append(modaction)

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
            user = users_by_username[modaction["target_author"]]
            user_metadata = json.loads(user.metadata_json)

            # Take a snapshot of the current user.
            snapshot = ExperimentThingSnapshot(
                {
                    "experiment_thing_id": user.id,
                    "object_type": user.object_type,
                    "experiment_id": user.experiment_id,
                    "metadata_json": user.metadata_json,
                }
            )
            self.db_session.add(snapshot)

            # Update the user based on the mod action taken.
            if self._is_tempban(modaction):
                # Temp ban was updated.
                user_metadata = {**user_metadata, **self._parse_temp_ban(user)}
                self.db_session.add(user)
            elif modaction["action"] == "banuser":
                # Escalated to permaban.
                user_metadata["ban_type"] = "permanent"
                if user.query_index == BannedUserQueryIndex.TBD:
                    user.query_index = BannedUserQueryIndex.IMPOSSIBLE
            elif modaction["action"] == "unbanuser":
                # User was unbanned.
                user_metadata["ban_type"] = "unbanned"
                if user.query_index == BannedUserQueryIndex.TBD:
                    user.query_index = BannedUserQueryIndex.IMPOSSIBLE

            user.metadata_json = json.dumps(user_metadata)
            self.db_session.add(user)

    def _get_account_age(self, account_created):
        now_utc = datetime.utcnow().timestamp()
        age_days = (now_utc - account_created) / 86400
        if age_days < 7:
            return "weekling"
        else:
            return "oldster"

    def _get_condition(self, account_created):
        age_bucket = self._get_account_age(account_created)

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

            self.log.info("YO")
            self.log.info(newcomer_modactions)
            self.log.info(len(newcomer_modactions))
            self.log.info(type(newcomer_modactions))

            for newcomer in newcomer_modactions:
                # Make an API call here to get the account age.
                # This is required to assign condition/randomization to the newcomer.
                newcomer_id = newcomer['target_author']
                info = self._load_redditor_info(newcomer_id)
                condition = self._get_condition(info["object_created"])

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



              

                user_metadata = {
                    "condition": condition,
                    "randomization": randomization,
                    **self._parse_temp_ban(newcomer),
                }
                user = {
                    "id": uuid.uuid4().hex,
                    "thing_id": newcomer["target_author"],
                    "experiment_id": self.experiment.id,
                    "object_type": ThingType.USER.value,
                    "object_created": info["object_created"],
                    "query_index": BannedUserQueryIndex.TBD,
                    "metadata_json": json.dumps(user_metadata),
                }
                newcomer_ets.append(user)

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
                "Error in BanneduserExperimentController::assign_randomized_conditions",
                e,
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
            A dict with details about the temporary ban, or empty dict if the action is not a temp ban.
            Note that `ban_start_time` and `ban_end_time` are UNIX timestamps in UTC.

        Example result:
            {
                "ban_duration_days": 30,
                "ban_reason": "Bad behavior",
                "ban_type": "temporary",
                "ban_start_time": 1704154715,
                "ban_end_time": 1705277915,
            }
        """
        days = self._parse_days(modaction)
        if days is None:
            return {}

        starts_at = int(modaction["created_utc"])
        ends_at = starts_at + (days * 86400)

        return {
            "ban_duration_days": days,
            "ban_reason": modaction["description"],
            "ban_start_time": starts_at,
            "ban_type": "temporary",
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

        m = re.search(r"(\d+) days", modaction["details"], re.IGNORECASE)
        return int(m.group(1)) if m else None

    def update_experiment(self):

        accounts_needing_messages = self._get_accounts_needing_interventions()
        self.log.info(
            "Experiment {0}: identified {1} accounts needing interventions. Sending messages now...".format(
                self.experiment.name, len(accounts_needing_messages)
            )
        )
        self._send_intervention_messages(accounts_needing_messages)

    def _get_accounts_needing_interventions(self):
        """Gets accounts that need interventions
        details about the ban.

        Returns:
            A dict with users that are enrolled in the study, that have not had messages sent yet, where the ban happened more than twelve hours ago.
        """

        # variable is named 'twelve' and not something more generic because this is a crucial part of experiment

        twelve_hours_ago = datetime.utcnow() - timedelta(hours=12)

        return (
            self.db_session.query(ExperimentThing)
            .filter(
                and_(
                    ExperimentThing.object_type == ThingType.USER.value,
                    ExperimentThing.experiment_id == self.experiment.id,
                    ExperimentThing.query_index == BannedUserQueryIndex.TBD,
                    ExperimentThing.object_created < twelve_hours_ago,
                )
            )
            .order_by(ExperimentThing.created_at)
            .all()
        )

    ## TODO: this is the same as format_message in NewcomerMessagingExperimentController in messaging_experiment_controller.. should this be abstracted out into the parent class? Should this be abstracted out? or not touch working code?
    def _format_intervention_message(self, experiment_thing):
        """Format intervention message for an thing, given the arm that it is in. This reads from the experiment_settings (the YAML file in /config/experiments/.

        Args:
            experiment_thing: The ExperimentThing for a tempbanned user.

        Returns:
            A dict with message subject and body.

        Example result:
            {
                "subject": "Tempban Message",
                "message": "You have been temporarily banned...",
            }
        """
        metadata_json = json.loads(experiment_thing.metadata_json)
        account_info = {"username": experiment_thing.thing_id}
        arm = metadata_json["arm"]
        cond = self.experiment_settings["conditions"][self.get_condition()]
        if arm not in cond["arms"].keys():
            raise ExperimentConfigurationError(
                "In the experiment '{0}', the '{1}' condition fails to include information about the '{2}' arm, despite having randomizations assigned to it".format(
                    self.experiment_name, self.get_condition(), arm
                )
            )
        if cond["arms"][arm] is None:
            return None
        message_subject = cond["arms"][arm]["pm_subject"].format(**account_info)
        message_body = cond["arms"][arm]["pm_text"].format(**account_info)
        return {"subject": message_subject, "message": message_body}

    def _send_intervention_messages(self, experiment_things):
        """Format intervention message for an thing, given the arm that it is in. This reads from the experiment_settings (the YAML file in /config/experiments/.

        Args:
            experiment_thing: The ExperimentThing for a tempbanned user.

        Returns:
            A dict with message subject and body.

        Example result:
            {
                "subject": "Tempban Message",
                "message": "You have been temporarily banned...",
            }
        """
        self.db_session.execute(
            "Lock Tables experiment_actions WRITE, experiment_things WRITE, message_logs WRITE"
        )
        message_results = []
        try:
            mc = MessagingController(self.db_session, self.r, self.log)
            action = "SendMessage"
            messages_to_send = []
            for experiment_thing in experiment_things:
                message = self._format_intervention_message(experiment_thing)
                ## if it's a control group, log inaction
                ## and do nothing, otherwise add to messages_to_send
                if message is None:
                    metadata = json.loads(experiment_thing.metadata_json)
                    metadata["message_status"] = "sent"
                    ea = ExperimentAction(
                        experiment_id=self.experiment.id,
                        action=action,
                        action_object_type=ThingType.USER.value,
                        action_object_id=experiment_thing.id,
                        metadata_json=json.dumps(metadata),
                    )
                    experiment_thing.query_index = BannedUserQueryIndex.COMPLETE
                    experiment_thing.metadata_json = json.dumps(metadata)
                    self.db_session.add(ea)
                else:
                    message["account"] = experiment_thing.thing_id
                    messages_to_send.append(message)

            # send messages_to_send
            message_results = mc.send_messages(
                messages_to_send,
                "BannedUserMessagingExperiment({0})::_send_intervention_messages".format(
                    self.experiment_name
                ),
            )

            # iterate through message_result, linked with experiment_things
            for experiment_thing in experiment_things:
                if experiment_thing.thing_id in message_results.keys():
                    message_result = message_results[experiment_thing.thing_id]

                    metadata = json.loads(experiment_thing.metadata_json)
                    update_records = False

                    message_errors = 0
                    if "errors" in message_result:
                        message_errors = len(message_result["errors"])

                    ## TAKE ACTION WITH INVALID USERNAME
                    ## (add an action and UPDATE THE EXPERIMENT_THING)
                    ## TO INDICATE THAT THE ACCOUNT DOESN'T EXIST
                    ## NOTE: THE MESSAGE ATTEMPT WILL BE LOGGED
                    ## SO YOU DON'T HAVE TO LOG AN ExperimentAction
                    ## Ignore other errors
                    ## (since you will want to retry in those cases)
                    if message_errors > 0:
                        for error in message_result["errors"]:
                            invalid_username = False
                            if error["error"] == "invalid username":
                                invalid_username = True

                            if invalid_username:
                                metadata["message_status"] = "nonexistent"
                                metadata["survey_status"] = "nonexistent"
                                experiment_thing.query_index = (
                                    BannedUserQueryIndex.IMPOSSIBLE
                                )
                                update_records = True
                    ## if there are no errors
                    ## add an ExperimentAction and
                    ## update the experiment_thing metadata
                    else:
                        metadata["message_status"] = "sent"
                        experiment_thing.query_index = BannedUserQueryIndex.COMPLETE
                        update_records = True

                    if update_records:
                        metadata_json = json.dumps(metadata)
                        ea = ExperimentAction(
                            experiment_id=self.experiment.id,
                            action=action,
                            action_object_type=ThingType.USER.value,
                            action_object_id=experiment_thing.id,
                            metadata_json=metadata_json,
                        )
                        self.db_session.add(ea)
                        experiment_thing.metadata_json = metadata_json
            self.db_session.commit()
        except Exception as e:
            self.db_session.execute("UNLOCK TABLES")
            self.log.error(
                "Error in BannedUserExperimentController::_send_intervention_messages",
                extra=sys.exc_info()[0],
            )
            return []
        self.db_session.execute("UNLOCK TABLES")

        return message_results
