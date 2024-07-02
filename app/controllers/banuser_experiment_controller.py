import praw
import inspect, os, sys, uuid # set the BASE_DIR
import simplejson as json
import datetime, yaml, time, csv
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
import sqlalchemy
from dateutil import parser
from utils.common import *
from app.models import Base, SubredditPage, Subreddit, Post, ModAction, PrawKey, Comment
from app.models import Experiment, ExperimentThing, ExperimentAction, ExperimentThingSnapshot
from app.models import EventHook
from sqlalchemy import and_, or_, not_, asc, desc
from app.controllers.messaging_controller import MessagingController
from app.controllers.experiment_controller import *
from collections import defaultdict

### DESCRIPTION OF THIS EXPERIMENT CONTROLLER
# This experiment controller should ..

#### CALLBACK BEHAVIOR: enroll_new_participants
## 1. ....

#### REGULARLY SCHEDULED JOB BEHAVIOR (intervention): update_experiment
## 1....

#### REGULARLY SCHEDULED POST-STUDY SURVEY BEHAVIOR (followup): (run from update_experiment)
## 1. ...


### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']

## TODO: This is currently designed to manage a newcomers-only experiment
## For this to be used in all of the student studies, this will need to be
## refactored into a general messaging experiment controller

class BanuserExperimentController(ExperimentController):
    def __init__(self, experiment_name, db_session, r, log, required_keys = ["event_hooks"]):

        super().__init__(experiment_name, db_session, r, log, required_keys)

        ## LOAD MESSENGER CONTROLLER
        #self.message_controller = >....

#    def update_experiment(self):
#        pass

#    def run_interventions(self, eligible_accounts):
#        pass

#    def assign_randomized_conditions(self, accounts):
#        pass

#    def make_control_nonaction(self, experiment_thing, account):
#        pass

#    def send_message(self, experiment_thing, account):
#        pass

    ## Accepts a list of account IDs
    # and returns a list of information about comment authors'
    # past participation in this particular experiment
    # NOTE: because multiple versions of this experiment may assign multiple treatments 
    #       to a single  account, it uses the thing_id field as the account ID 
    #       and the id field as a generated unique ID for the specific record. 
    #       This is different from how other experiments currently use the id field.
    # TODO: Refactor other experiments to use the thing_id field, and conduct a
    #       data migration to set thing_id across the CivilServant database    
   # def accounts_not_already_in_experiment(self, accounts):

    def previously_enrolled(self, account_usernames):

        # step two: get information about past participation in the experiment
        past_participation = defaultdict(list)
        for et_account in self.db_session.query(ExperimentThing).filter(and_(
            ExperimentThing.object_type == ThingType.USER.value,
            ExperimentThing.experiment_id == self.experiment.id,
            ExperimentThing.thing_id.in_(account_usernames)
            )).all():
                past_participation[et_account.thing_id].append(et_account)
        
        return past_participation


    """
        callback methods must pass in these 2 arguments: 
            self: an instance of callee class
            instance: an instance of caller class
    """
    ## FIND_BANNED_USERS:
    # Listen to callback and process new mod actions acquired.
    # Identify banned users in those mod actions, and start the
    # process of determining if they are eligible to be enrolled
    # in the study
    ## Called after ModeratorController.archive_mod_action_page

    def find_banned_users(self, instance):
        if(instance.fetched_subreddit_id != self.experiment_settings['subreddit_id']):
            return
        self.log.info("Successfully Ran Event Hook to BanuserExperimentController::find_banned_users. Caller: {0}".format(str(instance)))


