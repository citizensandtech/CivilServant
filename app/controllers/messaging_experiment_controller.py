import praw
import inspect, os, sys # set the BASE_DIR
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
from sqlalchemy import and_, or_
from app.controllers.subreddit_controller import SubredditPageController
from app.controllers.experiment_controller import ExperimentController

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']

class MessagingExperimentController(ExperimentController):
    def __init__(self, experiment_name, db_session, r, log, required_keys = ["newcomer_maximum_age_in_days"]):

        super().__init__(experiment_name, db_session, r, log, required_keys)

        ## LOAD MESSENGER CONTROLLER
        #self.message_controller = >....

#    def update_experiment(self):
#        pass

#    def run_interventions(self, eligible_accounts):
#        pass

#    def assign_randomized_conditions(self, accounts):
#        pass

#    def account_eligible(self, account):
#        pass

#    def make_control_nonaction(self, experiment_thing, account):
#        pass

#    def send_message(self, experiment_thing, account):
#        pass

#    
