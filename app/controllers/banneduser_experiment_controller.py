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

### BANNED USER EXPERIMENT CONTROLLER


### DESCRIPTION OF THIS EXPERIMENT CONTROLLER
# This experiment controller should:
# 1. Observe modactions to identify and enroll new participants
# 2. Randomly assign these users to receive different types of private messages
# 3. TBD

#### CALLBACK BEHAVIOR: enroll_new_participants
## 1. Find banned users ("action": "banuser")
## 2. Check eligibility from ban status (temporary bans only; no permabans) 
##    -- Temporary ban: ("details": "1 days" or "30 days")
##    -- Permanent ban: ("details": "permanent")


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



    def previously_enrolled_users(self):
        previously_enrolled = defaultdict(list)
        for et in self.db_session.query(Experiment).filter(and_(
            ExperimentThing.experiment_id == self.experiment.id,
            ExperimentThing.object_type == ThingType.USER.value,
        )).order_by(ExperimentThing.created_at).all():
            previously_enrolled[et.thing_id].append(et)

        return previously_enrolled
            










    #### FIND ELIGIBLE NEWCOMERS

    # Accepts a list of modactions
    # and returns a list of users who have been newly temporarly banned
    # ('newly' as in: we haven't noticed them before)

    def find_eligible_newcomers(self, modactions):



        ### LOGIC for finding temporary bans

        def is_tempban(modaction):
            return (modaction['action'] == 'banuser') and ("days" in modaction['details'])



        ### LOGIC for finding previously enrolled participants

        previously_enrolled_user_ids = self.previously_enrolled_users().keys()

        def is_not_previously_enrolled(modaction):
            return modaction['target_author'] not in previously_enrolled_user_ids



        ### iterate over modactions and find eligible newcomers

        eligible_newcomers = {}

        for modaction in modactions:

            if(is_tempban(modaction) and is_not_previously_enrolled(modaction)):

                eligible_newcomers[modaction['target_author']] = modaction
                # TODO: handle logic if the same user has multiple ban events 

        return eligible_newcomers






    def assign_randomized_conditions(self, new_tempbans):
        self.log.info("###### Assigning randomized conditions")

        self.db_session.execute("Lock Tables experiments WRITE, experiment_things WRITE")
        try:
            for newcomer in new_tempbans:
                self.log.info(newcomer)

                # WRITE STUFF

        except(Exception) as e:
           self.log.error("Error in BanneduserExperimentController::assign_randomized_conditions", extra=sys.exc_info()[0])
           return []
        finally:
           self.db_session.execute("UNLOCK TABLES")

        return




    ################################################### 
    ################################################### 
    ################################################### 
    ## ENROLL NEW PARTICIPANTS
    ## Called from ModeratorController.archive_mod_action_page

    def enroll_new_participants(self, instance):



        ### STEP 1. FIND ELIGIBLE NEWCOMERS

        self.log.info("Experiment {0}: scanning modactions in subreddit {1} to look for temporary bans".format(self.experiment.name, self.experiment_settings['subreddit_id']))

        eligible_newcomers = self.find_eligible_newcomers(instance.fetched_mod_actions)

   
        ### STEP 2. ASSIGN RANDOMIZED CONDITIONS
        
        self.log.info("Assigning randomized conditions to eligible newcomers")

        self.assign_randomized_conditions(eligible_newcomers)



       
        if instance.fetched_subreddit_id != self.experiment_settings["subreddit_id"]:
            return
        self.log.info(
            "Successfully Ran Event Hook to BanneduserExperimentController::enroll_new_participants. Caller: {0}".format(
                str(instance)
            )
        )
