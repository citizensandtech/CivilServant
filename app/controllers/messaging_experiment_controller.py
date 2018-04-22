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
from sqlalchemy import and_, or_
from app.controllers.subreddit_controller import SubredditPageController
from app.controllers.experiment_controller import *
from collections import defaultdict

### DESCRIPTION OF THIS EXPERIMENT CONTROLLER
# This experiment controller should observe commenters identified by
# A set of criteria and should randomly assign those commenters to
# receive a private message. Then, survey_followup_in_days later,
# accounts that were in the experiment should receive a further
# private message that includes a followup survey.

#### CALLBACK BEHAVIOR: enroll_new_participants
## 1. enroll new participants
## 2. check eligibility from newcomer perspective (r/feminism only)
## 3. check eligibility based on previous participation in the experiment (r/feminism and r/iama)
## 4. assign a participant to a condition in the experiment
##    and label the participant as not having yet received an intervention

#### REGULARLY SCHEDULED JOB BEHAVIOR (intervention): update_experiment
## 1. find participants that haven't received a condition action (including control group)
## 2. take the appropriate condition action
## 3. record the condition action and flag them as having received the condition
## 4. If the account is nonexistent, record that they were ineligible

#### REGULARLY SCHEDULED POST-STUDY SURVEY BEHAVIOR (followup): (run from update_experiment)
## 1. Identify participants who are eligible to receive a survey and who haven't
## 2. Send them a survey, or if that fails, record that they couldn't receive it

## NOTE: The Message Controller stores a MessageLog whenever we *attempt* to send a message
##       ExperimentActions should only be stored when we take an action we do not intend to retry
##       If we intend to retry an action, do not store an ExperimentAction about it and it will be
##       tried again

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']

## TODO: This is currently designed to manage a newcomers-only experiment
## For this to be used in all of the student studies, this will need to be
## refactored into a general messaging experiment controller

class MessagingExperimentController(ExperimentController):
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
    #       data migration to set thing_id across the CivilServant database    def accounts_not_already_in_experiment(self, accounts):

    def previously_enrolled(self, account_usernames):
        comment_author_ids = set()
        # step one: get author IDs from user database
        for user in self.db_session.query(User).filter(
            User.name.in_(account_usernames)).all():
            comment_author_ids.add(user.id)

        # step two: get information about past participation in the experiment
        past_participation = defaultdict(list)
        for et_account in self.db_session.query(ExperimentThing).filter(and_(
            ExperimentThing.object_type == 4,
            ExperimentThing.experiment_id == self.experiment.id,
            ExperimentThing.thing_id.in_(comment_author_ids)
            )).all():
                past_participation[et_account.thing_id].append(et_account)
        
        return past_participation


    """
        callback methods must pass in these 2 arguments: 
            self: an instance of callee class
            instance: an instance of caller class
    """
    ## ENROLL_NEW_PARTICIPANTS:
    # Listen to callback and process new comments acquired.
    # Identify commenters in those comments, and start the
    # process of determining if they are eligible to be enrolled
    # in the study
    ## Called from CommentController.fetch_last_thousand_comments
    def enroll_new_participants(self, instance):
        if(instance.last_subreddit_id != self.experiment_settings['subreddit_id']):
            return
        self.log.info("Successfully Ran Event Hook to MessagingExperimentController::enroll_new_participants. Caller: {0}".format(str(instance)))


## This MessagingExperimentController enrols participants who are first-time
## commenters in a community.
class NewcomerMessagingExperimentController(MessagingExperimentController):
    def __init__(self, experiment_name, db_session, r, log, required_keys = [
        "event_hooks", "newcomer_period_interval_days", "newcomer_supplemental_json_file"]):
        super().__init__(experiment_name, db_session, r, log, required_keys)

        ## check if the newcomer supplemental json file exists
        if(os.path.isfile(os.path.join(
            BASE_DIR, self.experiment_settings['newcomer_supplemental_json_file']))!=True):
            raise ExperimentConfigurationError(
                "In the experiment '{0}', the option newcomer_supplemental_jsonfile points to a nonexistent file: {1}".format(
                    experiment_name,
                    self.experiment_settings['newcomer_supplemental_json_file']
                ))

    def update_experiment(self):
        pass

    ## Accepts a list of comments
    # and returns a list of comment authors who are newcomers
    # as defined by the experiment setting: newcomer_first_comment_in_max_days
    # note that it's okay for there to be 
    def identify_newcomers(self, comments):
        current_date = datetime.datetime.utcnow()
        previous_commenters = set()
        newcomer_period_start = current_date - datetime.timedelta(
            days = self.experiment_settings['newcomer_period_interval_days'])
        
        ## FIRST CHECK ANY SPECIFIED SUPPLEMENTARY FILES FOR PREVIOUS COMMENTERS
        with open(self.experiment_settings['newcomer_supplemental_json_file'], "r") as f:
            for line in f:
                item = json.loads(line)
                item['created'] = datetime.datetime.utcfromtimestamp(float(item['created_utc']))
                if(item['created'] > newcomer_period_start):
                    previous_commenters.add(item['author'])

        ## NOW CHECK THE DATABASE FOR PREVIOUS COMMENTERS
        for comment in self.db_session.query(Comment).filter(and_(
            Comment.subreddit_id == self.experiment_settings['subreddit_id'],
            Comment.created_at > newcomer_period_start)):
            comment_data = json.loads(comment.comment_data)
            previous_commenters.add(comment_data['author'])

        ## NOW RETURN ANY NEWCOMER AUTHORS
        author_comments = defaultdict(list)
        for comment in comments:
            author_comments[comment['author']].append(comment)
        for author, comments in author_comments.items():
            author_comments[author] = sorted(comments, key=lambda x: x['created_utc'])

        return [{"author": author, "comment": author_comments[author][0]} 
            for author in author_comments.keys() 
                if author not in previous_commenters]


        # comment_thing = ExperimentThing(
        #     experiment_id = self.experiment.id,
        #     object_created = datetime.datetime.fromtimestamp(comment.created_utc),
        #     object_type = ThingType.COMMENT.value,
        #     id = comment.id,
        #     metadata_json = json.dumps({"group":"treatment", "arm":"arm_"+str(treatment_arm),
        #                                 "condition":condition,
        #                                 "randomization": metadata['randomization'],
        #                                 "submission_id":submission.id})
        # )

    def get_condition(self):
        if("main" not in self.experiment_settings['conditions'].keys()):
            self.log.error("Condition 'main' missing from configuration file.")
            raise Exception("Condition 'main' missing from configuration file")
        return "main"

    ## ASSIGN RANDOMIZED CONDITIONS
    ## Check to see if we have assigned these accounts in the past
    ## and give them assignments if necessary,
    ## tagging them as not having received the intervention
    ## Log an ExperimentAction with the assignments
    ## If you are out of available randomizations, throw an error
    def assign_randomized_conditions(self, newcomer_comments):
        condition = self.get_condition()

        newcomer_authors = [x['author'] for x in newcomer_comments]

        previously_enrolled = self.previously_enrolled(newcomer_authors)
        matched_newcomers = list(previously_enrolled.keys())

        self.db_session.execute("Lock Tables experiments WRITE, experiment_things WRITE")

        newcomer_ets = []
        newcomers_without_randomization = 0
        next_randomization = self.experiment_settings['conditions'][condition]['next_randomization']
        for newcomer in newcomer_comments:
            if newcomer['author'] not in matched_newcomers:
                et_metadata = {}
                
                # NOW ASSIGN THE RANDOMIZATION
                next_randomization = self.experiment_settings['conditions'][condition]['next_randomization']
                
                ## if there are no remaining randomizations, log the error,
                ## break from the loop, and continue
                if(next_randomization is not None and 
                    next_randomization >= len(self.experiment_settings['conditions'][condition]['randomizations'])):
                    next_randomization = None
                    newcomers_without_randomization += 1
                
                if(next_randomization is not None):
                    randomization = self.experiment_settings['conditions'][condition]['randomizations'][next_randomization]
                    self.experiment_settings['conditions'][condition]['next_randomization'] += 1

                    et_metadata = {
                        "condition": condition,
                        "randomization": randomization,
                        "submission_id": newcomer['comment']['link_id'],
                        "comment_id":newcomer['comment']['id'],
                        "arm": "arm_" + str(randomization['treatment']),
                        "message_status": "TBD",
                        "survey_status": "TBD"
                    }
                    # NOW SAVE AN EXPERIMENT THING
                    newcomer_ets.append({
                        "id": uuid.uuid4().hex,
                        "thing_id": newcomer['author'],
                        "experiment_id": self.experiment.id,
                        "object_type": ThingType.USER.value,
                        # we don't have account creation info
                        # at this stage, and it would take more queries to get
                        "object_created": None, 
                        "query_index": "Intervention TBD",
                        "metadata_json": json.dumps(et_metadata)
                    })
        if(newcomers_without_randomization > 0 ):
            self.log.error("NewcomerMessagingExperimentController Experiment {0} has run out of randomizations from '{1}' to assign.".format(self.experiment_name, condition))
        self.db_session.insert_retryable(ExperimentThing, newcomer_ets)

        self.experiment.experiment_settings = json.dumps(self.experiment_settings)
        self.db_session.commit()
        self.log.info("Assigned randomizations to {0} commenters: [{1}]".format(
            len(newcomer_ets),
            ",".join([x['thing_id'] for x in newcomer_ets])
        ))
        self.db_session.execute("UNLOCK TABLES")
        

    """
        callback methods must pass in these 2 arguments: 
            self: an instance of callee class
            instance: an instance of caller class
    """
    ## ENROLL_NEW_PARTICIPANTS:
    # Listen to callback and process new comments acquired.
    # Identify commenters in those comments, and start the
    # process of determining if they are eligible to be enrolled
    # in the study
    ## Called from CommentController.fetch_last_thousand_comments
    ## CHECK THE LATEST COMMENTS TO SEE WHICH ARE NEWCOMERS
    ## THEN CHECK TO SEE WHICH HAVE BEEN PREVIOUSLY ENROLLED IN THE EXPERIMENT
    ## THEN ASSIGN THEM RANDOMIZATIONS
    ## THEN HAND THINGS OFF TO BE PICKED UP BY update_experiment()

    def enroll_new_participants(self, instance):
        if(instance.last_subreddit_id != self.experiment_settings['subreddit_id']):
            return

        self.log.info("Successfully Ran Event Hook to MessagingExperimentController::enroll_new_participants. Caller: {0}".format(str(instance)))

        newcomers = self.identify_newcomers(instance.last_queried_comments)
        self.assign_randomized_conditions(newcomers)










        




