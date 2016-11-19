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
from app.models import Experiment, ExperimentThing, ExperimentAction
from sqlalchemy import and_, or_
from app.controllers.subreddit_controller import SubredditPageController
import numpy as np

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']

class StickyCommentExperimentController:
    def __init__(self, experiment_name, db_session, r, log):
        self.db_session = db_session
        self.log = log

        required_keys = ['subreddit', 'ama_comment_text', "nonama_comment_text",
                         'username', 'start_time', 'ama_conditions',
                         'nonama_conditions', 'max_eligibility_age', 'min_eligibility_age']

        experiment_file_path = os.path.join(BASE_DIR, "config", "experiments", experiment_name) + ".yml"
        with open(experiment_file_path, 'r') as f:
            try:
                experiment_config_all = yaml.load(f)
            except yaml.YAMLError as exc:
                self.log.error("Failure loading experiment yaml {0}".format(experiment_file_path), str(exc))
                sys.exit(1)
        if(ENV not in experiment_config_all.keys()):
            log.error("Cannot find experiment settings for {0} in {1}".format(ENV, experiment_file_path))
            sys.exit(1)

        experiment_config = experiment_config_all[ENV]
        for key in required_keys:
            if key not in experiment_config.keys():
                self.log.error("Value missing from {0}: {1}".format(experiment_file_path, key))
                sys.exit(1)

        experiment = self.db_session.query(Experiment).filter(Experiment.name == experiment_name).first()
        if(experiment is None):
            ## LOAD RANDOMIZED CONDITIONS (see CivilServant-Analysis)
            with open(os.path.join(BASE_DIR, "config", "experiments", experiment_config['ama_conditions']), "r") as f:
                reader = csv.DictReader(f)
                ama_conditions = []
                for row in reader:
                    ama_conditions.append(row)

            with open(os.path.join(BASE_DIR, "config", "experiments", experiment_config['nonama_conditions']), "r") as f:
                reader = csv.DictReader(f)
                nonama_conditions = []
                for row in reader:
                    nonama_conditions.append(row)
            
            settings = {
                "ama_comment_text": experiment_config['ama_comment_text'],
                "nonama_comment_text": experiment_config['nonama_comment_text'],
                "username": experiment_config['username'],
                "subreddit": experiment_config['subreddit'],
                "subreddit_id": experiment_config['subreddit_id'],
                "max_eligibility_age": experiment_config['max_eligibility_age'],
                "next_ama_condition":  0,
                "ama_conditions": ama_conditions,
                "next_nonama_condition": 0,
                "nonama_conditions": nonama_conditions
            }
            experiment = Experiment(
                name = experiment_name,
                controller = "StickyCommentExperimentController",
                start_time = parser.parse(experiment_config['start_time']),
                end_time = parser.parse(experiment_config['end_time']),
                settings_json = json.dumps(settings)
            )
            self.db_session.add(experiment)
            self.db_session.commit()
        
        ### SET UP INSTANCE PROPERTIES
        self.experiment = experiment
        experiment_settings = json.loads(self.experiment.settings_json)
        
        self.ama_conditions        = experiment_settings['ama_conditions']
        self.nonama_conditions     = experiment_settings['nonama_conditions']
        self.next_ama_condition    = experiment_settings['next_ama_condition']
        self.next_nonama_condition = experiment_settings['next_nonama_condition']

        self.experiment_name = experiment_name

        self.subreddit = experiment_config['subreddit']
        self.subreddit_id = experiment_config['subreddit_id']
        self.username = experiment_config['username']
        self.ama_comment_text = experiment_config['ama_comment_text']
        self.nonama_comment_text = experiment_config['nonama_comment_text']
        self.max_eligibility_age = experiment_config['max_eligibility_age']
        self.min_eligibility_age = experiment_config['min_eligibility_age']
        self.r = r

        ## LOAD SUBREDDIT PAGE CONTROLLER
        self.subreddit_page_controller = SubredditPageController(self.subreddit,self.db_session, self.r, self.log)

    ## main scheduled job
    ## TODO: TEST THIS METHOD IN UNIT TESTS
    def update_experiment(self):
        eligible_submissions = {}
        for submission in self.get_eligible_objects():
            eligible_submissions[submission.id] = submission
        
        rvals = []
        for experiment_thing in self.assign_randomized_conditions(eligible_submissions.values()):
            condition = int(json.loads(experiment_thing.metadata_json)['condition']['treatment'])
            if(condition == 1):
                rval = self.make_sticky_post(eligible_submissions[experiment_thing.id])
            elif(condition == 0):
                rval = self.make_control_nonaction(eligible_submissions[experiment_thing.id])
            if(rval is not None):
                rvals.append(rval)
        return rvals

    ## TODO: some of this code should be refactored
    ## into the code for get_eligible_objects
    def submission_acceptable(self, submission):
        if(submission is None):
            ## TODO: Determine what to do if you can't find the post
            self.log.error("Can't find experiment {0} post {1}".format(self.subreddit, submission.id))
            return False            

        ## Avoid Acting if the Intervention has already been recorded
        if(self.db_session.query(ExperimentAction).filter(and_(
            ExperimentAction.experiment_id      == self.experiment.id,
            ExperimentAction.action_object_type == ThingType.SUBMISSION.value,
            ExperimentAction.action_object_id   == submission.id,
            ExperimentAction.action             == "Intervention")).count() > 0):
                self.log.info("Experiment {0} post {1} already has an Intervention recorded".format(
                    self.experiment_name, 
                    submission.id))            
                return False

        ## Avoid Acting if an identical sticky comment already exists
        for comment in submission.comments:
            if(comment.stickied and (comment.body == self.ama_comment_text or comment.body == self.nonama_comment_text)):
                self.log.info("Experiment {0} post {1} already has a sticky comment {2}".format(
                    self.experiment_name, 
                    submission.id,
                    comment.id))
                return False

        ## Avoid Acting if the submission is not recent enough
        curtime = time.time()
#        if((curtime - submission.created_utc) > 10800):
        if((curtime - submission.created_utc) > self.max_eligibility_age):
            self.log.info("Submission created_utc {0} is {1} seconds greater than current time {2}, exceeding the max eligibility age of {3}. Declining to Add to the Experiment".format(
                submission.created_utc,
                curtime - submission.created_utc,
                curtime,
                self.max_eligibility_age))
            experiment_action = ExperimentAction(
                experiment_id = self.experiment.id,
                praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
                action = "NonIntervention:MaxAgeExceeded",
                action_object_type = ThingType.SUBMISSION.value,
                action_object_id = submission.id
            )
            return False
        return True

    def make_control_nonaction(self, submission):
        if(self.submission_acceptable(submission) == False):
            return None
        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action = "Intervention",
            action_object_type = ThingType.SUBMISSION.value,
            action_object_id = submission.id,
            metadata_json = json.dumps({"group":"control"})
        )
        self.db_session.add(experiment_action)
        self.db_session.commit()
        self.log.info("Experiment {0} applied control condition to post {1}".format(
            self.experiment_name, 
            submission.id
        ))
        return experiment_action.id

    def is_ama(self, submission):
        flair = []
        if submission.json_dict['link_flair_css_class']:
            flair = submission.json_dict['link_flair_css_class'].split()
        ama = False
        if "ama" in flair:
            ama = True
        return ama

    def make_sticky_post(self, submission):
        if(self.submission_acceptable(submission) == False):
            return None

        ## FIRST STEP: DETERMINE AMA STATUS
        ## TODO: refactor to check this only once
        is_ama = self.is_ama(submission)
        if(is_ama):
            comment_text = self.ama_comment_text
        else:
            comment_text = self.nonama_comment_text     

        comment = submission.add_comment(comment_text)
        distinguish_results = comment.distinguish(sticky=True)
        self.log.info("Experiment {0} applied treatment to post {1} (AMA = {2}). Result: {3}".format(
            self.experiment_name, 
            submission.id,
            is_ama,
            str(distinguish_results)
        ))

        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action_subject_type = ThingType.COMMENT.value,
            action_subject_id = comment.id,
            action = "Intervention",
            action_object_type = ThingType.SUBMISSION.value,
            action_object_id = submission.id,
            metadata_json = json.dumps({"group":"treatment", 
                "action_object_created_utc":comment.created_utc})
        )

        comment_thing = ExperimentThing(
            experiment_id = self.experiment.id,
            object_created = datetime.datetime.fromtimestamp(comment.created_utc),
            object_type = ThingType.COMMENT.value,
            id = comment.id,
            metadata_json = json.dumps({"group":"treatment","submission_id":submission.id})
        )

        self.db_session.add(comment_thing)
        self.db_session.add(experiment_action)
        self.db_session.commit()
        return distinguish_results

    ## TODO: REDUCE THE NUMBER OF API CALLS INVOLVED
    def get_eligible_objects(self):
        submissions = {}
        for submission in self.subreddit_page_controller.fetch_subreddit_page(PageType.NEW, return_praw_object=True):
            submissions[submission.id] = submission
        
        already_processed_ids = [thing.id for thing in 
            self.db_session.query(ExperimentThing).filter(and_(
                ExperimentThing.object_type==ThingType.SUBMISSION.value, 
                ExperimentThing.experiment_id == self.experiment.id,
                ExperimentThing.id.in_(submissions.keys()))).all()]

        eligible_submissions = []
        eligible_submission_ids = []
        curtime = time.time()

        for id, submission in submissions.items():
            if id in already_processed_ids:
                continue

            if((curtime - submission.created_utc) < self.min_eligibility_age):
                self.log.info("Submission {4} created_utc {0} is {1} seconds less than current time {2}, below the minimum eligibility age of {3}. Waiting to Add to the Experiment".format(
                    submission.created_utc,
                    curtime - submission.created_utc,
                    curtime,
                    self.min_eligibility_age,
                    submission.id))
                continue

            ### TODO: rule out eligibility based on age at this stage
            ### For now, we rule it out at the point of intervention
            ### Since it's easier to mock single objects in the tests
            ### Rather than a whole page of posts
            # if(self.submission_acceptable(submission) == False):
            #     continue
            eligible_submissions.append(submission)
            eligible_submission_ids.append(id)

        self.log.info("Experiment {0} Discovered {1}eligible submissions: {2}".format(
            self.experiment_name,
            len(eligible_submission_ids),
            json.dumps(eligible_submission_ids)))

        return eligible_submissions

    ## SEED CAN BE PASSED TO THE METHOD, TO AID IN UNIT TESTING
    def assign_randomized_conditions(self, submissions):
        if(submissions is None or len(submissions)==0):
            return []

        ## Assign experiment condition to objects
        experiment_things = []
        for submission in submissions:
            no_conditions_remain = False
            if(self.is_ama(submission)):
                try:
                    condition = self.ama_conditions[self.next_ama_condition]
                    self.next_ama_condition += 1
                except:
                    self.log.error("Experiment {0} has used its full stock of {1} AMA conditions. Cannot assign any further.".format(
                        self.experiment.name,
                        len(self.ama_conditions)
                    ))
                    no_conditions_remain = True
            else:
                try:
                    condition = self.nonama_conditions[self.next_nonama_condition]
                    self.next_nonama_condition += 1
                except:
                    self.log.error("Experiment {0} has used its full stock of {1} non-AMA conditions. Cannot assign any further.".format(
                        self.experiment.name,
                        len(self.nonama_conditions)
                    ))
                    no_conditions_remain = True

            if(no_conditions_remain):
                continue
            metadata = {}
            metadata['block.id']    = condition['block.id']
            metadata['block.size']  = int(condition['block.size'])
            metadata['condition']   = int(condition['treatment'])

            experiment_thing = ExperimentThing(
                id             = submission.id,
                object_type    = ThingType.SUBMISSION.value,
                experiment_id  = self.experiment.id,
                object_created = datetime.datetime.fromtimestamp(submission.created_utc),
                metadata_json  = json.dumps({"condition":condition})
            )
            self.db_session.add(experiment_thing)
            experiment_things.append(experiment_thing)
        
        ##UPDATE THE EXPERIMENT RECORD
        experiment_settings = json.loads(self.experiment.settings_json)
        experiment_settings['next_ama_condition'] = self.next_ama_condition
        experiment_settings['next_nonama_condition'] = self.next_nonama_condition
        self.experiment.settings_json = json.dumps(experiment_settings)

        self.log.info("Experiment {0}: assigned conditions to {1} submissions".format(self.experiment.name,len(experiment_things)))
        self.db_session.commit()
        return experiment_things

    def get_replies_for_removal(self, comment_objects):
        replies_for_removal = []
        for comment_object in comment_objects:
            comment_object.refresh()
            replies_for_removal = replies_for_removal + praw.helpers.flatten_tree(comment_object.replies)
        return replies_for_removal

    def get_all_experiment_comments(self):
        experiment_comments = self.db_session.query(ExperimentThing).filter(and_(
            ExperimentThing.experiment_id == self.experiment.id,
            ExperimentThing.object_type == ThingType.COMMENT.value
            )).all()
        return experiment_comments

    def get_all_experiment_comment_replies(self):
        experiment_comments = self.get_all_experiment_comments()
        experiment_comment_ids = [x.id for x in experiment_comments]
        comment_tree = Comment.get_comment_tree(self.db_session, sqlalchemyfilter = and_(
            Comment.subreddit_id == self.subreddit_id,
            Comment.created_at >= self.experiment.start_time,
            Comment.created_at <= self.experiment.end_time)) 
        experiment_comment_tree = [x for x in comment_tree['all_toplevel'].values() if x.id in experiment_comment_ids]
        
        all_experiment_comment_replies = []
        for comment in experiment_comment_tree:
            all_experiment_comment_replies = all_experiment_comment_replies + comment.get_all_children()
        return all_experiment_comment_replies

    def get_comment_objects_for_experiment_comment_replies(self, experiment_comment_replies):
        reply_ids = ["t1_" + x.id for x in experiment_comment_replies]
        comments = []
        if(len(reply_ids)>0):
            comments = self.r.get_info(thing_id = reply_ids)
        return comments

    def remove_replies_to_treatments(self):
        comments = self.get_comment_objects_for_experiment_comment_replies(
            self.get_all_experiment_comment_replies()
        )
        removed_comment_ids = []
        parent_submission_ids = set()
        for comment in comments:
            if(comment.banned_by is None):
                comment.remove()
                removed_comment_ids.append(comment.id)
                parent_submission_ids.add(comment.link_id)

        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            action        = "RemoveRepliesToTreatment",
            metadata_json = json.dumps({
                "parent_submission_ids":list(parent_submission_ids),
                "removed_comment_ids": removed_comment_ids
                })
        )
        self.db_session.add(experiment_action)
        self.db_session.commit()

        self.log.info("Experiment {experiment}: found {replies} replies to {treatments} treatment comments. Removed {removed} comments.".format(
            experiment = self.experiment.id,
            replies = len(comments),
            treatments = len(parent_submission_ids),
            removed = len(removed_comment_ids) 
        ))       
        return len(removed_comment_ids)     
