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

### DESCRIPTION OF THIS EXPERIMENT CONTROLLER
# This experiment controller should:
# 1. Observe modactions to identify new participants
#   -- Criteria include: temporarily banned users.
# 2. Randomly assign these users to receive different types of private messages
# 3. TBD

#### CALLBACK BEHAVIOR: enroll_new_participants
## 1. Find banned users
## 2. Check eligibility from ban status (temporary bans only)
## 3. Check eligibility based on TBD

#### REGULARLY SCHEDULED JOB BEHAVIOR (intervention): update_experiment
## 1....

#### REGULARLY SCHEDULED POST-STUDY SURVEY BEHAVIOR (followup): (run from update_experiment)
## 1. ...


### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
    "..",
    "..",
)
ENV = os.environ["CS_ENV"]

## TODO: This is currently designed to manage a banned users only experiment.


class ModactionExperimentController(ExperimentController):
    def __init__(
        self, experiment_name, db_session, r, log, required_keys=["event_hooks"]
    ):

        super().__init__(experiment_name, db_session, r, log, required_keys)

    """
        callback methods must pass in these 2 arguments: 
            self: an instance of callee class
            instance: an instance of caller class
    """

    ## enroll_new_participants:
    # Listen to callback and process new mod actions acquired.
    # These will be used

    def enroll_new_participants(self, instance):
        if instance.fetched_subreddit_id != self.experiment_settings["subreddit_id"]:
            return
        self.log.info(
            "Successfully Ran Event Hook to ModactionExperimentController::enroll_new_participants. Caller: {0}".format(
                str(instance)
            )
        )


class BanneduserExperimentController(ModactionExperimentController):
    def __init__(
        self, experiment_name, db_session, r, log, required_keys=["event_hooks"]
    ):
        super().__init__(experiment_name, db_session, r, log, required_keys)

    ## enroll_new_participants:
    # Listen to callback and process new mod actions acquired.
    # Identify banned users in those mod actions, and start the
    # process of determining if they are eligible to be enrolled
    # in the study
    ## Called after ModeratorController.archive_mod_action_page

    def enroll_new_participants(self, instance):
        if instance.fetched_subreddit_id != self.experiment_settings["subreddit_id"]:
            return
        self.log.info(
            "Successfully Ran Event Hook to BanneduserExperimentController::enroll_new_participants. Caller: {0}".format(
                str(instance)
            )
        )
