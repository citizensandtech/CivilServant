import abc
import json

from sqlalchemy import and_

from app.controllers.experiment_controller import ExperimentController
from app.models import ExperimentThing, ThingType


class ModactionExperimentController(ExperimentController, abc.ABC):
    """
    Mod Action experiment controller.

    An abstract class with methods that are useful for by any experiment on Reddit moderation actions.

    Callback methods must accept these 2 arguments:
        self: An instance of callee class
        instance: An instance of caller class
    """

    def __init__(
        self, experiment_name, db_session, r, log, required_keys=["event_hooks"]
    ):

        super().__init__(experiment_name, db_session, r, log, required_keys)

    @abc.abstractmethod
    def enroll_new_participants(self, instance):
        """Implement this method in a subclass for the experiment."""
        pass

    @abc.abstractmethod
    def _get_condition(self):
        """Get the condition name to use in this experiment."""

    def _check_condition(self, name):
        if name not in self.experiment_settings["conditions"]:
            error_message = f"Condition '{name}' missing from configuration file."
            self.log.error(error_message)
            raise Exception(error_message)

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
        return [u[0] for u in user_ids]

    def _populate_redditor_info(self, user_thing):
        """Load redditor information, if available, and save it to the ExperimentThing.

        Args:
            user_thing: ExperimentThing of type USER.

        Returns:
            The up-to-date ExperimentThing.
        """
        if user_thing.object_created is not None:
            # We already have the info we need.
            return user_thing
        redditor = self.r.redditor(user_thing.thing_id)

        # Grab the useful data about this user.
        user_thing.object_created = redditor.created_utc

        # Add new data to the current database transaction.
        self.db_session.add(redditor)

        return user_thing
