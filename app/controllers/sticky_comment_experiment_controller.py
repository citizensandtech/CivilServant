import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime, yaml, time
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
import sqlalchemy
from dateutil import parser
from utils.common import *
from app.models import Base, SubredditPage, Subreddit, Post, ModAction
from app.models import Experiment, ExperimentThing, ExperimentAction
from sqlalchemy import and_
from app.controllers.subreddit_controller import SubredditPageController
import numpy as np

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']

#class StickyCommentExperiment(Experiment):
    ## settings include:
    ## posting_user
    ## posting_praw_key
    ## 

class StickyCommentExperimentController:
    def __init__(self, experiment_name, db_session, r, log):
        self.db_session = db_session
        self.log = log

        required_keys = ['subreddit', 'comment_text', 'username', 
                         'start_time', 'end_time']

        experiment_file_path = os.path.join(BASE_DIR, "config", "experiments", experiment_name) + ".yml"
        with open(experiment_file_path, 'r') as f:
            try:
                experiment_config_all = yaml.load(f)
            except yaml.YAMLError as exc:
                log.error("Failure loading experiment yaml {0}".format(experiment_file_path), str(exc))
                sys.exit(1)
        if(ENV not in experiment_config_all.keys()):
            log.error("Cannot find experiment settings for {0} in {1}".format(ENV, experiment_file_path))
            sys.exit(1)

        experiment_config = experiment_config_all[ENV]
        for key in required_keys:
            if key not in experiment_config.keys():
                log.error("Value missing from {0}: {1}".format(experiment_file_path, key))
                sys.exit(1)

        experiment = self.db_session.query(Experiment).filter(Experiment.name == experiment_name).first()
        if(experiment is None):
            settings = {
                "comment_text": experiment_config['comment_text'],
                "username": experiment_config['username'],
                "subreddit": experiment_config['subreddit']
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
        
        self.experiment = experiment

        conn = reddit.connection.Connect()
        self.experiment_name = experiment_name

        self.subreddit = experiment_config['subreddit']
        self.username = experiment_config['username']
        self.comment_text = experiment_config['comment_text']
        self.r = r

        ## LOAD SUBREDDIT PAGE CONTROLLER
        self.subreddit_page_controller = SubredditPageController(self.subreddit,self.db_session, self.r, self.log)


    ## main scheduled job
    def update_experiment(self):
        eligible_posts = self.get_eligible_objects()
        #for condition in self.assign_randomized_conditions(eligible_posts):
            #if(#TRUE):
            #    apply_treatment
            #else:
            #    do_nothing
            #pass

    def make_sticky_post(self, submission_id):
        previously_stickied = False

        submission = self.r.get_submission(submission_id = submission_id)
        if(submission is None):
            ## TODO: Determine what to do if you can't find the post
            log.error("Can't find experiment {0} post {1}".format(self.subreddit, submission_id))
            sys.exit(1)

        ##  TODO: search the database
        ##  - get all comments
        for comment in submission.comments:
            if comment.stickied:
                ## TODO: Determine what to do if the intervention is a duplicate
                log.error("Experiment {0} post {1} already has a sticky comment {2}".format(
                    self.experiment_name, 
                    submission_id,
                    comment.id))
                sys.exit(1)
        comment = submission.add_comment(self.comment_text)
        distinguish_results = comment.distinguish(sticky=True)
        self.log("Experiment {0} applied treatment to post {1}. Result: {2}".format(
            self.experiment_name, 
            submission_id,
            str(distinguish_results)
        ))

        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action_subject_type = ThingType.COMMENT,
            action_subject_id = comment.id,
            action = "Intervention",
            action_object_type = ThingType.POST,
            action_object_id = submission.id
        )
        self.db_session.add(experiment_action)
        self.db_session.commit()
        return distinguish_results

    def get_eligible_objects(self):
        submissions = self.subreddit_page_controller.fetch_subreddit_page(PageType.NEW, return_praw_object=True)
        
        submission_ids = [submission.id for submission in submissions]

        already_processed_ids = [thing.id for thing in 
            self.db_session.query(ExperimentThing).filter(and_(
                ExperimentThing.object_type==ThingType.SUBMISSION.value, 
                ExperimentThing.id.in_(submission_ids))).all()]

        eligible_submissions = [submission for submission in submissions if submission.id not in already_processed_ids]
        self.log.info("Experiment {0} Discovered eligible submissions: {01}".format(
            self.experiment_name,
            json.dumps([submission.id for submission in eligible_submissions])))

        return eligible_submissions

    ## SEED CAN BE PASSED TO THE METHOD, TO AID IN UNIT TESTING
    def assign_randomized_conditions(self, submissions, seed = None):
        
        ## FIRST STEP: SET AND ARCHIVE THE RANDOM SEED
        if(seed is None):
            seed = time.time()
        np.random.seed(int(seed))
        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            action        = "SetRandomSeed",
            metadata_json = json.dumps({"seed":seed})
        )
        self.db_session.add(experiment_action)
        self.log.info("Experment {0}: set the random seed to {1}".format(self.experiment.id,seed))

        ## Assign experiment condition to objects
        experiment_things = []
        for submission in submissions:
            condition = bool(np.random.randint(0,2))
            experiment_thing = ExperimentThing(
                id             = submission.id,
                object_type    = ThingType.SUBMISSION.value,
                experiment_id  = self.experiment.id,
                object_created = datetime.datetime.fromtimestamp(submission.created_utc),
                metadata_json  = json.dumps({"condition":condition})
            )
            self.db_session.add(experiment_thing)
            experiment_things.append(experiment_thing)
        self.log.info("Experment {0}: assigned conditions to {1} submissions".format(self.experiment.id,len(experiment_things)))
        self.db_session.commit()
        return experiment_things

