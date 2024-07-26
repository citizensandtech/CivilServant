import abc

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
        return [u[0] for u in user_ids]
