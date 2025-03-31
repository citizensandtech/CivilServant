from datetime import datetime
from enum import Enum
import inspect
import json
import os
import re
import sys

import uuid
from sqlalchemy import and_, func


from app.controllers.experiment_controller import ExperimentConfigurationError
from app.controllers.modaction_experiment_controller import (
    ModactionExperimentController,
)
from app.controllers.messaging_controller import (
    MessagingController,
)
from app.models import (
    Comment,
    ExperimentAction,
    ExperimentThing,
    ExperimentThingSnapshot,
    ModAction,
    ThingType,
)

BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
    "..",
    "..",
)
ENV = os.environ["CS_ENV"]

SIX_MONTHS_IN_SECONDS = 6 * 30 * 24 * 60 * 60


class BannedUserQueryIndex(str, Enum):
    """Possible states of a banned user's query_index."""

    FIRST_BANSTART_PENDING = "First Banstart Intervention Pending"
    FIRST_BANSTART_COMPLETE= "First Banstart Intervention Complete"
    FIRST_BANSTART_IMPOSSIBLE = "First Banstart Intervention Impossible"
    SECOND_BANOVER_PENDING = "Second Banover Intervention Pending"
    SECOND_BANOVER_COMPLETE  = "Second Banover Intervention Complete"
    SECOND_BANOVER_IMPOSSIBLE  = "Second Banover Intervention Impossible"

class BanneduserExperimentController(ModactionExperimentController):
    """Banned user experiment controller.

    This experiment controller should:
    1. Observe modactions to identify and enroll new participants.
    2. Randomly assign these users to receive different types of private messages.
    3. Send a private message at the beginning of a temporary ban.
    """

    def __init__(
        self, experiment_name, db_session, r, log, required_keys=["event_hooks"]
    ):
        super().__init__(experiment_name, db_session, r, log, required_keys)
        self.log_prefix = f"{self.__class__.__name__} Experiment {experiment_name}:"

    def find_intervention_targets(
        self,
        instance,
        now_utc=int(datetime.utcnow().timestamp()),
    ):
        """Find intervention targets -- enroll new (banstart) participants in the experiment, assign banover intervention candidates.

        This is a callback that will be invoked declaratively.
        It is called by ModeratorController while running archive_mod_action_page, as noted in the experiment config YAML File.
        The `now_utc` timestamp may be set as a parameter for testing or replay purposes.

        Args:
            instance (Controller): the controller instance (standard for civilservant)
            now_utc (int): the current UNIX timestamp in seconds
        """
        if instance.fetched_subreddit_id != self.experiment_settings["subreddit_id"]:
            # Callback find_intervention_targets called due to modactions fetched from subreddit
            self.log.info(
                f"{self.log_prefix} Callback find_intervention_targets called but not needed for subreddit {instance.fetched_subreddit_id}"
            )
            return

        try:
            lock_id = f"{self.__class__.__name__}({self.experiment_name})::find_intervention_targets"
            with self.db_session.cooplock(lock_id, self.experiment.id):
                with self._new_modactions() as modactions:
                    
                    self.log.info(
                        f"{self.log_prefix} Scanning {len(modactions)} modactions in subreddit {self.experiment_settings['subreddit_id']} to look for temporary bans"
                    )

                    eligible_newcomers = self._find_first_banstart_candidates(modactions)
                    self.log.info(
                        f"{self.log_prefix} Identified {len(eligible_newcomers)} eligible newcomers / first_banstart candidates"
                    )

                    self.log.info(
                        f"{self.log_prefix} Assigning randomized conditions to eligible newcomers / first_banstart candidates"
                    )
                    self._enroll_first_banstart_candidates_with_randomized_conditions(now_utc, eligible_newcomers)


                    self.log.info(
                        f"{self.log_prefix} Scanning {len(modactions)} modactions in subreddit {self.experiment_settings['subreddit_id']} to look for unbans for second_banover candidates"
                    )
                    second_banover_candidates = self._find_second_banover_candidates(modactions)
                    self.log.info(
                        f"{self.log_prefix} Identified {len(second_banover_candidates)} second_banover candidates"
                    )

                    self.log.info(
                        f"{self.log_prefix} Assigning/designating second_banover candidates as candidates"
                    )
                    self._assign_second_banover_candidates(now_utc, second_banover_candidates)

                    self.log.info(
                        f"{self.log_prefix} Updating the state of existing participants"
                    )
                    self._update_existing_participants(now_utc, modactions)

                    self.log.info(
                        f"{self.log_prefix} Successfully Ran Event Hook to BanneduserExperimentController::find_intervention_targets. Caller: {instance}"
                    )

            # To minimize latency, trigger interventions immediately after enrolling new participants.
            self.update_experiment()
        except Exception as e:
            self.log.error(
                self.log_prefix,
                "Error in BanneduserExperimentController::find_intervention_targets",
                e,
            )
            self.db_session.rollback()

    def update_experiment(self):
        """Update loop for the banned user experiment.

        Check for freshly enrolled accounts, and send messages to them.
        """
        first_banstart_accounts_needing_messages = self._get_accounts_needing_first_banstart_interventions()
        second_banover_accounts_needing_messages = self._get_accounts_needing_second_banover_interventions()
        self.log.info(
            f"{self.log_prefix} Experiment {self.experiment.name}: identified {len(first_banstart_accounts_needing_messages)} accounts needing first banstart interventions and {len(second_banover_accounts_needing_messages)} accounts needing second banover interventions."
        )

        try:
            lock_id = f"{self.__class__.__name__}({self.experiment_name})::update_experiment"
            with self.db_session.cooplock(lock_id, self.experiment.id):
                self.log.info("{self.log_prefix} Sending first_banstart intervention messages...")
                self._send_first_banstart_intervention_messages(first_banstart_accounts_needing_messages)
                self.log.info("{self.log_prefix} Sending second_banover intervention messages...")
                self._send_second_banover_intervention_messages(second_banover_accounts_needing_messages)
        except Exception as e:
            self.log.error(
                self.log_prefix,
                "Error in BannedUserExperimentController::update_experiment",
                extra=sys.exc_info()[0],
            )
            self.db_session.rollback()
            return []

    def _find_first_banstart_candidates(self, modactions):
        """Filter a list of mod actions to find newcomers to the experiment.
        Starting with a list of arbitrary mod actions, select mod actions that:
        - are not for users already in the study,
        - are temporary bans,
        - do not appear to be bots, and
        - have existed for more than one week.

        Args:
            modactions: A list of mod actions.

        Returns:
            A filtered list of mod actions.
        """
        previously_enrolled_user_ids = set(self._previously_enrolled_user_ids())
        eligible_newcomers = {}
        for modaction in modactions:

            if (
                not self._is_enrolled(modaction, previously_enrolled_user_ids)
                and self._is_tempban(modaction)
                and not self._is_tempban_edit(
                    modaction
                )  # this is a proxy for 'is user already banned', as already banned users will receive ban edits as tempbans as well
                and self._is_valid_tempban_duration(modaction)
                and not self._is_bot(modaction)
                and not self._is_deleted(modaction)
            ):
                eligible_newcomers[modaction.target_author] = modaction

            # NOTE: If there are multiple mod actions for the same user who isn't yet enrolled,
            # we overwrite the previous action with the latest one.
            # This assumes that they are processed in order.


            # if an unbanuser happens immediately after a banuser is logged and before user is enrolled...
            if (
                not self._is_enrolled(modaction, previously_enrolled_user_ids)
                and self._is_unban(modaction)
            ):
                #  ... treat this as an accidental ban/unban by a mod, and don't enroll this ban/user in experiment
                if(modaction.target_author in eligible_newcomers):
                    del(eligible_newcomers[modaction.target_author])


        return list(eligible_newcomers.values())


    def _find_second_banover_candidates(self, modactions):
        """Filter a list of mod actions to find second_banover_candidates.
        Starting with a list of arbitrary mod actions, select mod actions that:
        - are for users already in the study,
        - temporary bans are over

        Args:
            modactions: A list of mod actions.

        Returns:
            A filtered list of mod actions.
        """
        previously_enrolled_user_ids = set(self._previously_enrolled_user_ids())
        first_banstart_complete_user_ids = set(self._get_first_banstart_complete_user_ids())
        second_banover_candidates = {}
        for modaction in modactions:

            # if an unbanuser happens immediately after a banuser is logged and before user is enrolled...
            if (
                    #self._is_enrolled(modaction, previously_enrolled_user_ids)
                    #and self._is_unban(modaction)
                self._is_unban(modaction)
                #and modaction.target_author in first_banstart_complete_user_ids # first_banstart intervention is complete. TODO: should this be in its own method?
            ):
                second_banover_candidates[modaction.target_author] = modaction

        return list(second_banover_candidates.values())

    def _update_existing_participants(self, now_utc, modactions):
        """Find mod actions that update the state of any current participants.
        This is for the sake of tracking participants in the database, not for performing interventions.

        Args:
            modactions: A list of mod actions.
        """
        previously_enrolled_user_ids = set(self._previously_enrolled_user_ids())

        # Find currently temporarily banned user records to update.
        users_by_username = {}
        for ut in self.db_session.query(ExperimentThing).filter(
            ExperimentThing.object_type == ThingType.USER.value,
            ExperimentThing.experiment_id == self.experiment.id,
        ):
            user_metadata = json.loads(ut.metadata_json)
            if user_metadata["ban_type"] == "temporary":
                users_by_username[ut.thing_id] = ut

        updated_users = []
        for modaction in modactions:
            # Skip mod actions that don't apply to a current participant.
            if not self._is_enrolled(modaction, previously_enrolled_user_ids):
                continue

            # Update the details of the ban, upgrade to a permanent ban, or remove the ban.
            # Assume that actions are in chronological order (last action per user wins).
            if modaction.action not in ["banuser", "unbanuser"]:
                continue

            # filter for modactions that are temporarily banned
            user = users_by_username.get(modaction.target_author)
            if not user:
                continue

            user_metadata = json.loads(user.metadata_json)

            updated_users.append(modaction)

        if not updated_users:
            return

        # Apply updates to corresponding `ExperimentThing`s.
        for modaction in updated_users:
            # NOTE: This could throw a KeyError, but it shouldn't according to how we build this dict.
            user = users_by_username[modaction.target_author]
            user_metadata = json.loads(user.metadata_json)

            # Take a snapshot of the current user.
            snapshot = ExperimentThingSnapshot(
                experiment_thing_id=user.thing_id,
                object_type=user.object_type,
                experiment_id=user.experiment_id,
                metadata_json=user.metadata_json,
            )
            self.db_session.add_retryable(snapshot)

            # Update the user based on the mod action taken.
            if self._is_tempban(modaction):
                # Temp ban was updated.
                user_metadata = {**user_metadata, **self._parse_temp_ban(modaction)}
            elif modaction.action == "banuser":
                # Escalated to permaban.
                user_metadata["ban_type"] = "permanent"
                user_metadata["actual_ban_end_time"] = -1  # spec from study
                if user.query_index == BannedUserQueryIndex.FIRST_BANSTART_PENDING:
                    user.query_index = BannedUserQueryIndex.FIRST_BANSTART_IMPOSSIBLE
                if user.query_index == BannedUserQueryIndex.SECOND_BANOVER_PENDING:
                    user.query_index = BannedUserQueryIndex.SECOND_BANOVER_IMPOSSIBLE
            elif modaction.action == "unbanuser":
                # User was unbanned.
                user_metadata["ban_type"] = "unbanned"
                user_metadata["actual_ban_end_time"] = int(now_utc)
                if user.query_index == BannedUserQueryIndex.FIRST_BANSTART_PENDING:
                    user.query_index = BannedUserQueryIndex.FIRST_BANSTART_IMPOSSIBLE
                # NOTE: upon an unban, _assign_second_banover_candidates will set query_index to SECOND_BANOVER_PENDING. So do not alter SECOND_BANOVER_PENDING values, otherwise banover interventions will not trigger.

            user.metadata_json = json.dumps(user_metadata)

            self.db_session.add_retryable(user)

        self.db_session.commit()

    def _get_condition(self, newcomer, now_utc):
        """Get the categorical condition for this new participant.

        Args:
            newcomer (ModAction): The `banuser` action that enters the user into the study.
            now_utc: Current timestamp in UTC.

        Returns: A named category for this participant.
        """
        ban_condition = self._get_ban_condition(newcomer)
        activity_condition = self._get_activity_condition(newcomer, now_utc)
        condition = f"{activity_condition}_{ban_condition}"
        self._check_condition(condition)
        return condition

    def _get_ban_condition(self, newcomer):
        """Categorize newcomers to the experiment based on ban duration.

        Args:
            newcomer (ModAction): The `banuser` action that enters the user into the study.

        Returns: A category representing the length of the ban.
        """
        mapping = {3: "threedays", 7: "sevendays", 14: "fourteendays", 30: "thirtydays"}
        ban_condition = mapping.get(self._parse_days(newcomer), "unknown")
        return ban_condition

    def _get_activity_condition(self, newcomer, now_utc):
        """Categorize newcomers to the experiment based on comment history:

        Args:
            newcomer (ModAction): The `banuser` action that enters the user into the study.
            now_utc: Current timestamp in UTC.

        Returns: A category based on the user's comment history.
            - `lurker` has not commented in known history
            - `lowremoval` has had relatively few comments removed by mods
            - `highremoval` has had comments removed by mods
        """

        # Calculate the timestamp for six months ago.
        six_months_ago_timestamp = int(now_utc - SIX_MONTHS_IN_SECONDS)

        number_of_comments_query = self.db_session.query(func.count(Comment.id)).filter(
            Comment.subreddit_id == self.experiment_settings["subreddit_id"],
            Comment.user_id == newcomer.target_author,
            Comment.created_utc >= six_months_ago_timestamp,
        )

        number_of_comments = number_of_comments_query.scalar()
        if number_of_comments == 0:
            return "lurker"

        modactions = (
            self.db_session.query(ModAction)
            .filter(
                ModAction.subreddit_id == self.experiment_settings["subreddit_id"],
                ModAction.target_author == newcomer.target_author,
                ModAction.created_utc >= six_months_ago_timestamp,
            )
            .order_by(ModAction.created_utc)
        )

        # Ignore comments that are removed and later approved.
        comments = set()
        for modaction in modactions:
            comment_id = modaction.target_fullname
            if modaction.action == "removecomment":
                comments.add(comment_id)
            elif modaction.action == "approvecomment":
                comments.discard(comment_id)

        number_of_removals = len(comments)
        removal_ratio = number_of_removals / number_of_comments

        removal_ratio_threshold = self.experiment_settings[
            "participant_activity_condition_removal_ratio_threshold"
        ]

        if removal_ratio <= removal_ratio_threshold:
            return "lowremoval"
        else:
            return "highremoval"

    def _enroll_first_banstart_candidates_with_randomized_conditions(self, now_utc, newcomers):
        """Assign randomized conditions to newcomers.
        Log an ExperimentAction with the assignments. (Messages will get sent later in update_experiment.)
        If there are no available randomizations, throw an error.

        Args:
            now_utc (int): the current datetime in UTC.
            newcomers: a list of tuples of `(mod action, redditor info)`.
        """
        # list of newcomer experiment_things to be added to db
        newcomer_ets = []
        newcomers_without_randomization = 0

        for newcomer in newcomers:
            condition = self._get_condition(newcomer, now_utc)

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
            self.experiment_settings["conditions"][condition]["next_randomization"] += 1

            user_metadata = {
                "condition": condition,
                "randomization": randomization,
                "arm": "arm_" + str(randomization["treatment"]),
                **self._parse_temp_ban(newcomer),
            }
            user = {
                "id": uuid.uuid4().hex,
                "thing_id": newcomer.target_author,
                "experiment_id": self.experiment.id,
                "object_type": ThingType.USER.value,
                "query_index": BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                "metadata_json": json.dumps(user_metadata),
            }

            newcomer_ets.append(user)

        if newcomers_without_randomization > 0:
            self.log.error(
                f"{self.log_prefix} BanneduserExperimentController Experiment {self.experiment_name} has run out of randomizations from '{condition}' to assign."
            )

        if len(newcomer_ets) > 0:
            self.db_session.insert_retryable(ExperimentThing, newcomer_ets)
            self.experiment.settings_json = json.dumps(self.experiment_settings)

        self.log.info(
            f"{self.log_prefix} Assigned randomizations to {len(newcomer_ets)} banned users: [{','.join([x['thing_id'] for x in newcomer_ets])}]"
        )

    def _assign_second_banover_candidates(self, now_utc, candidates):
        """Assign/update database for second_banover candidates.
        (If randomization is needed, it would happen here!)
        Log an ExperimentAction with the assignments. (Messages will get sent later in update_experiment.)
        If there are no available randomizations, throw an error.

        Args:
            now_utc (int): the current datetime in UTC.
            candidates: a list of tuples of `(mod action, redditor info)`.
        """

        candidate_ets = []
        for candidate in candidates:

            # get experiment_thing of candidate
            candidate_et = self.db_session.query(ExperimentThing).filter(
                and_(
                    ExperimentThing.experiment_id == self.experiment.id,
                    ExperimentThing.object_type == ThingType.USER.value,
                    ExperimentThing.thing_id == candidate.target_author,
                    ExperimentThing.query_index == BannedUserQueryIndex.FIRST_BANSTART_COMPLETE, # this is redundant but we do it anyways
                )
            ).first()

            if not candidate_et:
                self.log.error(
                    "Mismatch between _find_second_banover_candidates and _assign_second_banover_candidates. This shouldn't happen. This means that, somehow between querying  in _find_second_banover_candidates and this query, there's a difference. Maybe you're testing incorrectly?"
                )
                continue

            # update query_index of ExperimentThing to SECOND_BANOVER_PENDING
            candidate_et.query_index = BannedUserQueryIndex.SECOND_BANOVER_PENDING

            # update database with new ExperimentThing, but don't commit yet since we're in a loop
            self.db_session.add_retryable(candidate_et, commit=False)

            candidate_ets.append(candidate_et)

        self.db_session.commit()

        self.log.info(
            f"{self.log_prefix} Assigned second_banover pending status to {len(candidate_ets)} unbanned users: [{','.join([x['thing_id'] for x in candidate_ets])}]"
        )



    def _is_tempban(self, modaction):
        """Return true if an admin action is a temporary ban.

        For permanent bans, we expect `details` to be "permanent".
        For temporary bans, we expect the number of days, e.g. "7 days".
        """
        return modaction.action == "banuser" and "days" in modaction.details

    def _is_unban(self, modaction):
        """Return true if an admin action is an unban.

        Unbanuser messages are sent BOTH upon automatic ban expire as well as manual ban end.
        """
        return modaction.action == "unbanuser"
    

    def _is_tempban_edit(self, modaction):
        """Return true if an admin action is a temporary ban edit.

        For temporary bans, we expect the string "changed to" in `details`. E.g. "changed to 1 days".

        This calls `_is_tempban`, even if redundant, as to be unambiguous and not assume that modaction is already a tempban.
        """
        return self._is_tempban(modaction) and "changed to" in modaction.details

    def _is_valid_tempban_duration(self, modaction):
        """Return true if tempban duration is a valid duration (3, 7, 14, and 30) days."""
        return self._parse_days(modaction) in [3, 7, 14, 30]

    def _is_enrolled(self, modaction, enrolled_user_ids):
        """Return true if the target of an admin action is already enrolled."""
        return modaction.target_author in enrolled_user_ids

    def _is_bot(self, modaction):
        """Return true if the user appears to be a bot.

        This is currently a rudimentary approach. Account age is typically a better indicator.
        """
        return re.match(r".+bot$", modaction.target_author, re.IGNORECASE) != None

    def _is_deleted(self, modaction):
        """Return true if the target of a mod action is deleted."""
        return modaction.target_author == "[deleted]"


    def _parse_temp_ban(self, modaction):
        """Get details about the ban.

        Args:
            modaction: The moderation action for a temporary ban.

        Returns:
            A dict with details about the temporary ban, or empty dict if the action is not a temp ban.
            Note that `ban_start_time` and `actual_ban_end_time` are UNIX timestamps in UTC.
            `actual_ban_end_time` is set to None on initialization, -1 upon permaban, and to timestamp when unban event occurs.
        Example result:
            {
                "ban_duration_days": 30,
                "ban_reason": "Bad behavior",
                "ban_type": "temporary",
                "ban_start_time": 1704154715,
                "actual_ban_end_time": None,
            }
        """
        days = self._parse_days(modaction)
        if days is None:
            return {}

        # XXX: `created_utc` is sometimes a datetime and sometimes a timestamp!
        starts_at = modaction.created_utc
        if isinstance(starts_at, datetime):
            starts_at = starts_at.timestamp()
        starts_at = int(starts_at)

        return {
            "ban_duration_days": days,
            "ban_reason": modaction.description,
            "ban_start_time": starts_at,
            "ban_type": "temporary",
            "actual_ban_end_time": None,
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

        m = re.search(r"(\d+) days", modaction.details, re.IGNORECASE)
        return int(m.group(1)) if m else None

    
    def _get_first_banstart_complete_user_ids(self):
        """Get user IDs that have their first intervention complete.

        Returns:
            A list of user IDs.
        """
        user_ids = self.db_session.query(ExperimentThing.thing_id).filter(
            and_(
                ExperimentThing.experiment_id == self.experiment.id,
                ExperimentThing.object_type == ThingType.USER.value,
                ExperimentThing.query_index == BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
            )
        )
        return [u[0] for u in user_ids]

    def _get_accounts_needing_first_banstart_interventions(self):
        """Gets accounts that need first_banstart interventions.

        Returns:
            A list of users that are enrolled in the study, that have not had messages sent yet.
        """

        return (
            self.db_session.query(ExperimentThing)
            .filter(
                and_(
                    ExperimentThing.object_type == ThingType.USER.value,
                    ExperimentThing.experiment_id == self.experiment.id,
                    ExperimentThing.query_index == BannedUserQueryIndex.FIRST_BANSTART_PENDING,
                )
            )
            .order_by(ExperimentThing.created_at)
            .all()
        )

    def _get_accounts_needing_second_banover_interventions(self):
        """Gets accounts that need second_banover interventions.

        Returns:
            A list of users that are enrolled in the study, that were recently unbanned and need a banover message sent.
        """

        return (
            self.db_session.query(ExperimentThing)
            .filter(
                and_(
                    ExperimentThing.object_type == ThingType.USER.value,
                    ExperimentThing.experiment_id == self.experiment.id,
                    ExperimentThing.query_index == BannedUserQueryIndex.SECOND_BANOVER_PENDING,
                )
            )
            .order_by(ExperimentThing.created_at)
            .all()
        )



    ## TODO: this is the same as format_message in NewcomerMessagingExperimentController in messaging_experiment_controller.. should this be abstracted out into the parent class? Should this be abstracted out? or not touch working code?
    def _format_intervention_message(self, experiment_thing, intervention_type):
        """Format intervention message for an thing, given the arm that it is in. This reads from the experiment_settings (the YAML file in /config/experiments/.

        Args:
            experiment_thing: The ExperimentThing for a tempbanned user.
            intervention_type: String indicating type of intervention. Either 'first_banstart' or 'second_banover'.

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

        if(intervention_type == "first_banstart"):

            condition = metadata_json["condition"]
            arm = metadata_json["arm"]

            if condition not in self.experiment_settings["conditions"]:
                raise ExperimentConfigurationError(
                    f"In the experiment '{self.experiment_name}', the '{condition}' condition fails to exist in the configuration"
                )

            yml_cond = self.experiment_settings["conditions"][condition]
            if arm not in yml_cond["arms"]:
                raise ExperimentConfigurationError(
                    f"In the experiment '{self.experiment_name}', the '{condition}' condition fails to include information about the '{arm}' arm, despite having randomizations assigned to it"
                )
            if yml_cond["arms"][arm] is None:
                return None
            message_subject = yml_cond["arms"][arm]["pm_subject"].format(**account_info)
            message_body = yml_cond["arms"][arm]["pm_text"].format(**account_info)

        else:  # second_banover
            if second_banover_message not in self.experiment_settings:
                raise ExperimentConfigurationError(
                    f"In the experiment '{self.experiment_name}', the 'second_banover_message' entry fails to exist in the configuration"
                )
            message_subject = self.experiment_settings["second_banover_message"]["pm_subject"].format(**account_info)
            message_body = self.experiment_settings["second_banover_message"]["pm_text"].format(**account_info)

        return {"subject": message_subject, "message": message_body}


    def _send_first_banstart_intervention_messages(self, experiment_things):
        """Sends first banstart intervention messages.
        This is for users who have just been banned.

        Args:
            experiment_things: A list of ExperimentThings for users needing first_banstart interventions.
        """
        return self._send_intervention_messages(
            experiment_things,
            complete_status=BannedUserQueryIndex.FIRST_BANSTART_COMPLETE,
            impossible_status=BannedUserQueryIndex.FIRST_BANSTART_IMPOSSIBLE,
            intervention_type="first_banstart"
        )

    def _send_second_banover_intervention_messages(self, experiment_things):
        """Sends second banover intervention messages.
        This is for users whose ban has just ended.

        Args:
            experiment_things: A list of ExperimentThings for users needing second_banover interventions.
        """
        return self._send_intervention_messages(
            experiment_things,
            complete_status=BannedUserQueryIndex.SECOND_BANOVER_COMPLETE,
            impossible_status=BannedUserQueryIndex.SECOND_BANOVER_IMPOSSIBLE,
            intervention_type="second_banover"
        )
    

    def _send_intervention_messages(self, experiment_things, complete_status, impossible_status, intervention_type):
        """Sends appropriate intervention messages for a list of experiment things.

        Args:
            experiment_things: A list of ExperimentThings representing tempbanned users.
            complete_status: The BannedUserQueryIndex to use when message is sent successfully
            impossible_status: The BannedUserQueryIndex to use when message cannot be sent
            intervention_type: String indicating type of intervention. Either 'first_banstart' or 'second_banover'.

        Returns:
            A dict with server response from praw's send_message,
            or a dict with key 'error' if there was an error sending a message.

        Example result:
            {
                "LaLaLatour47": {
                    ....
                }
            }
        """
        mc = MessagingController(self.db_session, self.r, self.log)
        action = "SendMessage"
        messages_to_send = []
        for experiment_thing in experiment_things:
            message = self._format_intervention_message(experiment_thing, intervention_type)
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
                experiment_thing.query_index = complete_status
                experiment_thing.metadata_json = json.dumps(metadata)
                self.db_session.add_retryable(ea)
            else:
                message["account"] = experiment_thing.thing_id
                messages_to_send.append(message)

        self.log.info(
            f"{self.log_prefix} Sending {intervention_type} messages to {len(messages_to_send)} users: [{','.join([x['account'] for x in messages_to_send])}]"
        )
        # send messages_to_send
        message_results = mc.send_messages(
            messages_to_send,
            f"BannedUserMessagingExperiment({self.experiment_name})::_send_{intervention_type}_intervention_messages",
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
                            experiment_thing.query_index = impossible_status
                            update_records = True
                ## if there are no errors
                ## add an ExperimentAction and
                ## update the experiment_thing metadata
                else:
                    metadata["message_status"] = "sent"
                    experiment_thing.query_index = complete_status
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
                    self.db_session.add_retryable(ea, commit=False)
                    experiment_thing.metadata_json = metadata_json
        # NOTE: experiment_things also become updated in database with this commit method,
        # as they are sqlalchemy objects
        self.db_session.commit()
        return message_results
