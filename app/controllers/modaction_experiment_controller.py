import abc
from contextlib import contextmanager
import datetime
import json

from sqlalchemy import and_

from app.controllers.experiment_controller import ExperimentController
from app.models import ExperimentThing, ModAction, ThingType

MOD_ACTION_CURSOR_KEY = "last_modaction_timestamp"


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

    @abc.abstractmethod
    def _get_condition(self):
        """Get the condition name to use in this experiment."""

    @contextmanager
    def _new_modactions(self, should_save_cursor=True):
        """Load new mod actions.

        They should be recorded:
        - after the end of the experiment,
        - after the last processed mod action, if any, and
        - before the end of the experiment.

        The query has a few notable properties:
        - Use an exclusive range to avoid confusion related to processing the same time more than once.
        - Limit result size. Repeated runs will advance through large batches of mod actions.

        Args:
            should_save_cursor: whether to save the `last_modaction_timestamp` cursor in experiment settings

        Yields:
            A list of new mod actions for the experiment.
        """
        first_time = self.experiment.start_time + datetime.timedelta(seconds=1)
        window_start = max(first_time, self._last_modaction_time())
        window_end = self.experiment.end_time + datetime.timedelta(seconds=1)
        modactions = (
            self.db_session.query(ModAction)
            .filter(
                and_(
                    ModAction.created_utc > window_start,
                    ModAction.created_utc < window_end,
                )
            )
            .order_by(ModAction.created_utc.asc())
            .limit(500)
            .all()
        )

        # XXX: parse `action_data` and set *ephemeral* values on the model instance.
        for m in modactions:
            meta = json.loads(m.action_data).get("json_dict", {})
            for k, v in meta.items():
                if not hasattr(m, k):
                    setattr(m, k, v)

        yield modactions
        if len(modactions) > 0 and should_save_cursor:
            # NOTE: modactions are sorted by created_utc, so the last one is always the max timestamp.
            last_modaction = modactions[-1]
            self.experiment_settings[MOD_ACTION_CURSOR_KEY] = int(
                last_modaction.created_utc.timestamp()
            )
            self.experiment.settings_json = json.dumps(self.experiment_settings)
            self.db_session.add_retryable(self.experiment)

    def _last_modaction_time(self):
        return datetime.datetime.fromtimestamp(
            self.experiment_settings.get(MOD_ACTION_CURSOR_KEY, 0)
        )

    def _check_condition(self, name):
        """Check whether the named condition is configured for this experiment.

        If the experiment is not configured properly, log the error raise an exception.
        """
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
