import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime, yaml, time, csv
import uuid
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
import sqlalchemy
from dateutil import parser
from utils.common import *
from app.models import Base, SubredditPage, Subreddit, Post, ModAction, PrawKey, Comment, User
from app.models import Experiment, ExperimentThing, ExperimentAction, ExperimentThingSnapshot
from app.models import EventHook
from sqlalchemy import and_, or_
from app.controllers.subreddit_controller import SubredditPageController
import numpy as np

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']


class StickyCommentExperimentController:
    def __init__(self, experiment_name, db_session, r, log, required_keys = 
        ['subreddit', 'subreddit_id', 'username', 'conditions', 'controller', 
         'max_eligibility_age', 'min_eligibility_age']):

        self.db_session = db_session
        self.log = log
        self.r = r
        self.load_experiment_config(required_keys, experiment_name)
        
    def get_experiment_config(self, required_keys, experiment_name):        
        experiment_file_path = os.path.join(BASE_DIR, "config", "experiments", experiment_name) + ".yml"
        with open(experiment_file_path, 'r') as f:
            try:
                experiment_config_all = yaml.full_load(f)
            except yaml.YAMLError as exc:
                self.log.error("{0}: Failure loading experiment yaml {1}".format(
                    self.__class__.__name__, experiment_file_path), str(exc))
                sys.exit(1)
        if(ENV not in experiment_config_all.keys()):
            self.log.error("{0}: Cannot find experiment settings for {1} in {2}".format(
                self.__class__.__name__, ENV, experiment_file_path))
            sys.exit(1)

        experiment_config = experiment_config_all[ENV]
        for key in required_keys:
            if key not in experiment_config.keys():
                self.log.error("{0}: Value missing from {1}: {2}".format(
                    self.__class__.__name__, experiment_file_path, key))
                sys.exit(1)
        return experiment_config

    def load_experiment_config(self, required_keys, experiment_name):
        experiment_config = self.get_experiment_config(required_keys, experiment_name)
        experiment = self.db_session.query(Experiment).filter(Experiment.name == experiment_name).first()
        if(experiment is None):

            condition_keys = []

            ## LOAD RANDOMIZED CONDITIONS (see CivilServant-Analysis)
            for condition in experiment_config['conditions'].values():
                with open(os.path.join(BASE_DIR, "config", "experiments", condition['randomizations']), "r") as f:
                    reader = csv.DictReader(f)
                    randomizations = []
                    for row in reader:
                        randomizations.append(row)
                        condition['randomizations']  = randomizations

            experiment = Experiment(
                name = experiment_name,
                controller = self.__class__.__name__,
                start_time = parser.parse(experiment_config['start_time']),
                end_time = parser.parse(experiment_config['end_time']),
                settings_json = json.dumps(experiment_config)
            )
            self.db_session.add_retryable(experiment)
            #self.db_session.add(experiment)
            #self.db_session.commit()
        
        ### SET UP INSTANCE PROPERTIES
        self.experiment = experiment
        self.experiment_settings = json.loads(self.experiment.settings_json)
        

        self.experiment_name = experiment_name
        self.dry_run = experiment_config.get("dry_run", False)

        self.subreddit = experiment_config['subreddit']
        self.subreddit_id = experiment_config['subreddit_id']
        self.username = experiment_config['username']
        self.max_eligibility_age = experiment_config['max_eligibility_age']
        self.min_eligibility_age = experiment_config['min_eligibility_age']

        ## LOAD SUBREDDIT PAGE CONTROLLER
        self.subreddit_page_controller = SubredditPageController(self.subreddit,self.db_session, self.r, self.log)

        # LOAD EVENT HOOKS
        self.load_event_hooks(experiment_config)

    def load_event_hooks(self, experiment_config):
        if 'event_hooks' not in experiment_config:
            return

        hooks = experiment_config['event_hooks']

        now = datetime.datetime.utcnow()
        for hook_name in hooks:
            hook = self.db_session.query(EventHook).filter(
                EventHook.name == hook_name).first()
            if not hook:
                call_when_str = hooks[hook_name]['call_when']
                if call_when_str == "EventWhen.BEFORE":
                    call_when = EventWhen.BEFORE.value
                elif call_when_str == "EventWhen.AFTER":
                    call_when = EventWhen.AFTER.value
                else:
                    self.log.error("{0}: While loading event hooks, call_when string incorrectly formatted: {1}".format(
                        self.__class__.__name__, call_when_str))
                    sys.exit(1)

                hook_record = EventHook(
                    name = hook_name,
                    created_at = now,
                    experiment_id = self.experiment.id,
                    is_active = hooks[hook_name]['is_active'],
                    call_when = call_when,
                    caller_controller = hooks[hook_name]['caller_controller'],
                    caller_method = hooks[hook_name]['caller_method'],
                    callee_module = hooks[hook_name]['callee_module'],
                    callee_controller = hooks[hook_name]['callee_controller'],
                    callee_method = hooks[hook_name]['callee_method'])
                self.db_session.add_retryable(hook_record)
                #self.db_session.add(hook_record)
                #self.db_session.commit()

    ## main scheduled job
    def update_experiment(self):
        objs = self.set_eligible_objects() 
        eligible_objs = self.get_eligible_objects(objs, ThingType.SUBMISSION)
        return self.run_interventions(eligible_objs, ThingType.SUBMISSION)
    
    def run_interventions(self, eligible_objs, thing_type):
        results = []
        for experiment_thing, obj in self.assign_randomized_conditions(eligible_objs, thing_type):
            result = self.run_intervention(experiment_thing, obj, thing_type)
            if result is not None:
                results.append(result)
        return results
    
    def run_intervention(self, experiment_thing, obj, thing_type):
        condition = json.loads(experiment_thing.metadata_json)['condition']
        randomizable = json.loads(experiment_thing.metadata_json).get('randomizable')
        if randomizable:
            randomization = json.loads(experiment_thing.metadata_json).get('randomization')
            arm_label = "arm_" + str(randomization['treatment']) if randomization else ''
            intervene_fn_name = 'intervene_' + condition + '_' + arm_label
        else:
            intervene_fn_name = 'intervene_' + condition + '_default'
        intervene = getattr(self, intervene_fn_name)
        self.log.info("{0}: Experiment {1} {2} {3} intervention_method: {4}".format(
            self.__class__.__name__,
            self.experiment_name,
            thing_type.name.lower(),
            experiment_thing.id,
            intervene_fn_name))
        return intervene(experiment_thing, obj)

    ## Check the acceptability of a submission before acting
    def submission_acceptable(self, submission, action="Intervention"):
        if(submission is None):
            ## TODO: Determine what to do if you can't find the post
            self.log.error("{0}: Can't find experiment {1} post {2}".format(
                self.__class__.__name__, self.subreddit, submission.id))
            return False            

        ## Avoid Acting if the action has already been recorded
        if(self.db_session.query(ExperimentAction).filter(and_(
            ExperimentAction.experiment_id      == self.experiment.id,
            ExperimentAction.action_object_type == ThingType.SUBMISSION.value,
            ExperimentAction.action_object_id   == submission.id,
            ExperimentAction.action             == action)).count() > 0):
                self.log.info("{0}: Experiment {1} post {2} already has action {3} recorded".format(
                    self.__class__.__name__,
                    self.experiment_name, 
                    submission.id,
                    action))       
                return False

        ## possible comment texts to avoid
        all_experiment_messages = []
        for condition in self.experiment_settings['conditions'].values():
            for arm in condition['arms'].values():
                if type(arm) is str:
                    all_experiment_messages.append(arm)
                elif type(arm) is dict:
                    sticky_text_key = arm.get('sticky_text_key')
                    if sticky_text_key:
                        all_experiment_messages.append(self.experiment_settings[sticky_text_key])

        # Avoid Acting if an identical sticky comment already exists
        comments = getattr(submission, "comments", []) # needed for testing the StickyCommentMessagingExperimentController
        for comment in comments:
            if(hasattr(comment, "stickied") and comment.stickied and (comment.body in all_experiment_messages)):
                self.log.info("{0}: Experiment {1} post {2} already has a sticky comment {2}".format(
                    self.__class__.__name__,
                    self.experiment_name, 
                    submission.id,
                    comment.id))
                return False

        return True

    def make_control_nonaction(self, experiment_thing, submission, group="control", action="Intervention"):
        if(self.submission_acceptable(submission) == False):
            self.log.info("{0}: Submission {1} is unacceptable. Declining to make nonaction".format(
                self.__class__.__name__,
                submission.id))
            return None

        metadata      = json.loads(experiment_thing.metadata_json)
        treatment_arm = int(metadata['randomization']['treatment'])
        condition     = metadata['condition']
        
        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action = action,
            action_object_type = ThingType.SUBMISSION.value,
            action_object_id = submission.id,
            metadata_json = json.dumps({"group":group, "condition":condition,
                "arm":"arm_" + str(treatment_arm),
                "randomization": metadata['randomization'],
                "action_object_created_utc":None})
        )
        self.db_session.add_retryable(experiment_action)
        #self.db_session.add(experiment_action)
        #self.db_session.commit()
        self.log.info("{0}: Experiment {1} applied arm {2} to post {3} (condition = {4})".format(
            self.__class__.__name__,
            self.experiment_name, 
            treatment_arm,
            submission.id,
            condition
        ))
        return experiment_action.id

    def make_sticky_post(self, experiment_thing, submission, group="treatment", action="Intervention"):
        if(self.submission_acceptable(submission) == False):
            self.log.info("{0}: Submission {1} is unacceptable. Declining to make sticky post".format(
                self.__class__.__name__,
                submission.id))
            return None

        metadata = json.loads(experiment_thing.metadata_json)
        treatment_arm = int(metadata['randomization']['treatment'])
        condition     = metadata['condition']

        arm_config = self.experiment_settings['conditions'][condition]['arms']['arm_' + str(treatment_arm)]
        if type(arm_config) is str:
            comment_text = self.experiment_settings['conditions'][condition]['arms']["arm_" + str(treatment_arm)]
        else:
            sticky_text_key = arm_config['sticky_text_key']
            comment_text = self.experiment_settings[sticky_text_key]

        ## THIS METHOD IS FOR USE WHEN INTENDING TO TEST AN EXPERIMENT WITHOUT
        ## MAKING ANY ACTUAL COMMENTS ON REDDIT
        def _dry_run_add_comment():
            import collections, random, string
            return collections.namedtuple("Comment", ["id", "created_utc"])(
                "_" + "".join(random.choice(string.ascii_lowercase) for i in range(6)),
                datetime.datetime.utcnow().timestamp())

        ## TO USE THIS DRY RUN TEST CODE ON EXPERIMENTS WITHOUT self.dry_run
        ## AVAILABLE, UNCOMMENT THE FOLLOWING TWO LINES AND COMMENT OUT THE TWO
        # LINES AFTER IT
        #comment = _TEST_add_comment() #submission.add_comment(comment_text)
        ## distinguish_results = "DRY RUN DISTINGUISH: Assume successful" 

        if self.dry_run:
            comment = _dry_run_add_comment()
            distinguish_results = "DRY RUN DISTINGUISH: Assume successful" 
        else:
            comment = submission.add_comment(comment_text)
            distinguish_results = comment.distinguish(sticky=True)

        self.log.info("{0}: Experiment {1} applied arm {2} to post {3} (condition = {4}). Result: {5}".format(
            self.__class__.__name__,            
            self.experiment_name,
            treatment_arm, 
            submission.id,
            condition,
            str(distinguish_results)
        ))

        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action_subject_type = ThingType.COMMENT.value,
            action_subject_id = comment.id,
            action = action,
            action_object_type = ThingType.SUBMISSION.value,
            action_object_id = submission.id,
            metadata_json = json.dumps({"group":group, "condition":condition,
                "arm":"arm_" + str(treatment_arm),
                "randomization": metadata['randomization'],
                "action_object_created_utc":comment.created_utc})
        )

        comment_thing = ExperimentThing(
            experiment_id = self.experiment.id,
            object_created = datetime.datetime.fromtimestamp(comment.created_utc),
            object_type = ThingType.COMMENT.value,
            id = comment.id,
            metadata_json = json.dumps({"group":group, "arm":"arm_"+str(treatment_arm),
                                        "condition":condition,
                                        "randomization": metadata['randomization'],
                                        "submission_id":submission.id})
        )

        self.db_session.add_retryable([comment_thing, experiment_action])
        #self.db_session.add(comment_thing)
        #self.db_session.add(experiment_action)
        #self.db_session.commit()
        return distinguish_results


    def set_eligible_objects(self):
        objs = self.subreddit_page_controller.fetch_subreddit_page(PageType.NEW, return_praw_object=True)
        return objs
    

    ## TODO: REDUCE THE NUMBER OF API CALLS INVOLVED
    ## in the future possibly merge with submission_acceptable
    def get_eligible_objects(self, objs, thing_type, require_flair=False,
                             min_eligibility_age_enabled=True,
                             max_eligibility_age_enabled=True):
        
        objs_dict = {o.id:o for o in objs}

        # TODO Determine how a non-locking select statement could possibly
        # result in a deadlock. Made retryable to handle that for now.
        @retryable(backoff=True, rollback=True, session=self.db_session)
        def _fetch_already_processed_objects():
            if len(objs_dict) == 0:
                return []
            return [thing.id for thing in 
                self.db_session.query(ExperimentThing).filter(and_(
                    ExperimentThing.object_type==thing_type.value, 
                    ExperimentThing.experiment_id == self.experiment.id,
                    ExperimentThing.id.in_(list(objs_dict.keys())))).all()]
        already_processed_ids = _fetch_already_processed_objects()

        eligible_objs = []
        eligible_obj_ids = []
        curtime = datetime.datetime.now().timestamp()
        aged_out_objs = []

        for id, obj in objs_dict.items():
            if id in already_processed_ids:
                continue

            if min_eligibility_age_enabled and (curtime - obj.created_utc) < self.min_eligibility_age:
                self.log.info("{0}: {1} {2} created_utc {3} is {4} seconds less than current time {5}, below the minimum eligibility age of {6}. Waiting to Add to the Experiment".format(
                    self.__class__.__name__,
                    thing_type.name.title(),
                    obj.id,
                    obj.created_utc,
                    curtime - obj.created_utc,
                    curtime,
                    self.min_eligibility_age))
                continue
            
            if require_flair and not obj.json_dict["link_flair_css_class"]:
                self.log.info("{0}: {1} {2} does not have any flair applied. Waiting to Add to the Experiment".format(
                    self.__class__.__name__,
                    thing_type.name.title(),
                    obj.id,
                    obj.created_utc,
                    curtime - obj.created_utc,
                    curtime,
                    self.min_eligibility_age))
                continue

            ## THE FOLLOWING IF STATEMENT IS NOT TESTED IN THE UNIT TESTS
            #if(obj.created_utc < self.experiment.start_time):
            #    self.log.info("{0}: {1} created_utc {2} is earlier than experiment start time {3}. Declining to Add to the Experiment".format(
            #        self.__class__.__name__,
            #        thing_type.name.title(),
            #        obj.created_utc,
            #        self.experiment.start_time))
            #    continue

            if max_eligibility_age_enabled and (curtime - obj.created_utc) > self.max_eligibility_age:
                aged_out_objs.append(obj)
                #self.log.info("{0}: {1} created_utc {2} is {3} seconds greater than current time {4}, exceeding the max eligibility age of {5}. Declining to Add to the Experiment".format(
                #    self.__class__.__name__,
                #    thing_type.name.title(),
                #    obj.created_utc,
                #    curtime - obj.created_utc,
                #    curtime,
                #    self.max_eligibility_age))
                continue


            eligible_objs.append(obj)
            eligible_obj_ids.append(id)

        self.log.info("{0}: Experiment {1} Discovered {2} eligible {3}s: {4}".format(
            self.__class__.__name__,
            self.experiment_name,
            len(eligible_obj_ids),
            thing_type.name.lower(),
            json.dumps(eligible_obj_ids)))

        self.log.info("{0}: Experiment {1} Discovered {2} {3}s over max age of {4} seconds.".format(
            self.__class__.__name__,
            self.experiment_name,
            len(aged_out_objs),
            thing_type.name.lower(),
            self.max_eligibility_age
        ))

        return eligible_objs
    
    # By default all objects are randomizable but override this if a
    # subcontroller may need to create non-randomized experiment things
    def randomizable(self, obj, thing_type):
        return True

    def build_condition_experiment_thing(self, obj, thing_type, condition, randomizable, randomization):
        return ExperimentThing(
            id             = obj.id,
            object_type    = thing_type.value,
            experiment_id  = self.experiment.id,
            object_created = datetime.datetime.fromtimestamp(obj.created_utc),
            metadata_json  = json.dumps({
                "randomizable": randomizable,
                "randomization": randomization,
                "condition": condition})
        )
    
    # Subclasses will make use of obj and thing_type
    def get_randomization(self, obj, thing_type, label):
        condition = self.experiment_settings['conditions'][label]
        try:
            randomization = condition['randomizations'][condition['next_randomization']]
            self.experiment_settings['conditions'][label]['next_randomization'] += 1
            return randomization
        except:
            self.log.error("{0}: Experiment {1} has used its full stock of {2} {3} conditions. Cannot assign any further.".format(
                self.__class__.__name__,
                self.experiment.name,
                len(condition['randomizations']),
                label
            ))
            return None

    def assign_randomized_conditions(self, objs, thing_type):
        if(objs is None or len(objs)==0):
            return []
        ## Assign experiment condition to objects
        experiment_things = []
        combined = []
        for obj in objs:
            label = self.identify_condition(obj)
            if label is None:
                continue
            randomization = None
            randomizable = self.randomizable(obj, thing_type)
            if randomizable:
                randomization = self.get_randomization(obj, thing_type, label)
                if randomization is None:
                    continue
            experiment_thing = self.build_condition_experiment_thing(obj, thing_type, label, randomizable, randomization)
            experiment_things.append(experiment_thing)
            combined.append((experiment_thing, obj))

        self.experiment.settings_json = json.dumps(self.experiment_settings)
        self.db_session.add_retryable(experiment_things)
        
        self.log.info("{0}: Experiment {1}: assigned conditions to {2} {3}s".format(
            self.__class__.__name__,
            self.experiment.name,
            len(experiment_things),
            thing_type.name.lower()))

        return combined

        
    #######################################################
    ## CODE FOR REMOVING REPLIES TO STICKY COMMENTS
    #######################################################

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
        self.db_session.add_retryable(experiment_action)
        #self.db_session.add(experiment_action)
        #self.db_session.commit()

        self.log.info("{controller}: Experiment {experiment}: found {replies} replies to {treatments} treatment comments. Removed {removed} comments.".format(
            controller = self.__class__.__name__,
            experiment = self.experiment.id,
            replies = len(comments),
            treatments = len(parent_submission_ids),
            removed = len(removed_comment_ids) 
        ))       
        return len(removed_comment_ids)

    ## IDENTIFY THE CONDITION, IF ANY, THAT APPLIES TO THIS OBSERVATION
    ## THIS METHOD SHOULD SUPPORT CASES WHERE THERE ARE MULTIPLE CONDITIONS, INCLUSIVE
    ## OR WHERE ONLY SOME OBSERVATIONS SHOULD BE PART OF THE EXPERIMENT
    ## RETURN: LABEL NAME FOR THE CONDITION IN QUESTION
    def identify_condition(self, obj):
        for label in self.experiment_settings['conditions'].keys():
            detection_method = getattr(self, "identify_"+label)
            if detection_method(obj):
                return label
        return None

    #######################################################
    ## CODE FOR QUERYING INFORMATION ABOUT EXPERIMENT OBJECTS
    #######################################################

    def archive_experiment_submission_metadata(self):
        submission_ids = ["t3_" + thing.id for thing in 
            self.db_session.query(ExperimentThing).filter(and_(
                ExperimentThing.object_type==ThingType.SUBMISSION.value, 
                ExperimentThing.experiment_id == self.experiment.id))]
        if(len(submission_ids)==0):
            self.log.info("{controller}: Experiment {experiment}: Logged metadata for 0 submissions.".format(
                controller = self.__class__.__name__,
                experiment = self.experiment.id
            ))       
            return []

        snapshots = []
        for submission in self.r.get_info(thing_id = submission_ids):
            snapshot = {"score":submission.score,
                        "num_reports":submission.num_reports,
                        "user_reports":len(submission.user_reports),
                        "mod_reports":len(submission.mod_reports),
                        "num_comments":submission.num_comments,
                        }
            experiment_thing_snapshot = ExperimentThingSnapshot(
                experiment_id = self.experiment.id,
                experiment_thing_id = submission.id,
                object_type = ThingType.SUBMISSION.value,
                metadata_json = json.dumps(snapshot)
            )
            #self.db_session.add(experiment_thing_snapshot)
            snapshots.append(experiment_thing_snapshot)

        self.db_session.add_retryable(snapshots)
        #self.db_session.commit()

        self.log.info("{controller}: Experiment {experiment}: Logged metadata for {submissions} submissions.".format(
            controller = self.__class__.__name__,
            experiment = self.experiment.id,
            submissions = len(snapshots)
        ))       

        return snapshots
        

#### Class for experiment with different randomization for AMAs
#### Used to test subclassess of StickyCommentExperimentController
class AMAStickyCommentExperimentController(StickyCommentExperimentController):
    def __init__(self, experiment_name, db_session, r, log):
        required_keys = ['subreddit', 'subreddit_id', 'username', 
                         'start_time', 'end_time',
                         'max_eligibility_age', 'min_eligibility_age',
                         'conditions']
        
        super().__init__(experiment_name, db_session, r, log, required_keys)


    ###############################
    ### EXPERIMENT-SPECIFIC METHODS
    ###############################

    def is_ama(self, submission):
        flair = []
        if submission.json_dict['link_flair_css_class']:
            flair = submission.json_dict['link_flair_css_class'].split()
        ama = False
        if "ama" in flair:
            ama = True
        return ama

    def identify_ama(self, submission):
        return self.is_ama(submission)

    def identify_nonama(self, submission):
        return self.is_ama(submission) != True

    ## CONTROL GROUP (AMA)
    def intervene_nonama_arm_0(self, experiment_thing, submission):
        return self.make_control_nonaction(experiment_thing, submission)
        
    ## TREATMENT GROUP (AMA)
    def intervene_nonama_arm_1(self, experiment_thing, submission):
        return self.make_sticky_post(experiment_thing, submission)

    ## CONTROL GROUP (NONAMA)
    def intervene_ama_arm_0(self, experiment_thing, submission):
        return self.make_control_nonaction(experiment_thing, submission)
    
    ## TREATMENT GROUP (NONAMA)
    def intervene_ama_arm_1(self, experiment_thing, submission):
        return self.make_sticky_post(experiment_thing, submission)


# Created for the 2020.02 replication of the r/science study to account for:
#   * updated criteria for detecting an r/science type of AMA
#   * adding the presence of flair as a study eligibility criterion
class AMA2020StickyCommentExperimentController(AMAStickyCommentExperimentController):
    def is_ama(self, submission):
        self_domain = "self.%s" % self.subreddit
        ama = submission.json_dict["domain"] == self_domain
        return ama

    def get_eligible_objects(self, objs, thing_type):
        return super().get_eligible_objects(objs, thing_type, require_flair=True)


class SubsetStickyCommentExperimentController(StickyCommentExperimentController):
    def __init__(self, experiment_name, db_session, r, log):
        required_keys = ['subreddit', 'subreddit_id', 'username', 
                         'start_time', 'end_time',
                         'max_eligibility_age', 'min_eligibility_age',
                         'conditions']
        
        super().__init__(experiment_name, db_session, r, log, required_keys)

    def identify_considered_domain(self, submission):
        return submission.domain in self.experiment_settings['conditions']['considered_domain']['matched_domains']

    def intervene_considered_domain_arm_0(self, experiment_thing, submission):
        return self.make_control_nonaction(experiment_thing, submission)
    
    def intervene_considered_domain_arm_1(self, experiment_thing, submission):
        return self.make_sticky_post(experiment_thing, submission)

    def intervene_considered_domain_arm_2(self, experiment_thing, submission):
        return self.make_sticky_post(experiment_thing, submission)
    


### THIS SUBCLASS OF THE EXPERIMENT DOES NOT TAKE A SCHEDULED JOB
### INSTEAD IT UPDATES AS PART OF A CALLBACK PROCESS
class FrontPageStickyCommentExperimentController(StickyCommentExperimentController):
    def __init__(self, experiment_name, db_session, r, log):
        required_keys = ['subreddit', 'subreddit_id', 'username', 
                         'start_time', 'end_time',
                         'max_eligibility_age', 'min_eligibility_age',
                         'conditions', 'event_hooks']

        super().__init__(experiment_name, db_session, r, log, required_keys)

    def set_eligible_objects(self, instance):
        return instance.posts

    # takes in dictionary {id: praw objects}
    def archive_eligible_submissions(self, eligible_submissions):
        existing_post_ids = set([sub.id for sub in self.db_session.query(Post).filter(
            Post.id.in_(list(eligible_submissions.keys())))])

        # list of praw objects
        to_archive_posts = [eligible_submissions[sid] for sid in eligible_submissions if sid not in existing_post_ids]
            
        new_posts = []
        for post in to_archive_posts:
            post_info = post.json_dict if("json_dict" in dir(post)) else post['data'] ### TO HANDLE TEST FIXTURES
            new_post = Post(
                    id = post_info['id'],
                    subreddit_id = post_info['subreddit_id'].strip("t5_"), # janky
                    created = datetime.datetime.fromtimestamp(post_info['created_utc']),        
                    post_data = json.dumps(post_info))
            new_posts.append(new_post)
            #self.db_session.add(new_post)
        self.db_session.add_retryable(new_posts)
        #self.db_session.commit()

    def get_eligible_objects(self, object_list, thing_type):
      subreddit_id_fullname = "t5_"+ self.subreddit_id
      objects_in_subreddit = [obj for obj in object_list if obj.subreddit_id==subreddit_id_fullname]
      return super(FrontPageStickyCommentExperimentController, self).get_eligible_objects(objects_in_subreddit, thing_type)

    ## main callback job
    # differs from parent class's update_experiment with:
    #   - different method signature. "instance" variable used 
    #       because update_experiment is a callback
    #       (instance is an instance of the caller object)
    #   - calls different set_eligible_objects, which takes in "instance" variable
    def callback_update_experiment(self, instance):  ####
        eligible_submissions = {}
        objs = self.set_eligible_objects(instance)  ####
        
        eligible_objects = self.get_eligible_objects(objs, ThingType.SUBMISSION)
        eligible_submissions = {sub.id: sub for sub in eligible_objects}

        self.archive_eligible_submissions(eligible_submissions) ####
        return self.run_interventions(eligible_submissions.values(), ThingType.SUBMISSION)

    #override this method so it doesn't do anything
    def update_experiment(self):
      return

    def identify_frontpage_post(self, submission):
        # they are all frontpage posts  ???
        return True

    ## CONTROL GROUP
    def intervene_frontpage_post_arm_0(self, experiment_thing, submission):
        self.log.info("{0}: Experiment {1} post {2} intervene_frontpage_post_arm_0".format(
            self.__class__.__name__,
            self.experiment_name,
            submission.id))
        return self.make_control_nonaction(experiment_thing, submission, group="control")
        
    ## TREATMENT GROUP
    def intervene_frontpage_post_arm_1(self, experiment_thing, submission):
        self.log.info("{0}: Experiment {1} post {2} intervene_frontpage_post_arm_1".format(
            self.__class__.__name__,
            self.experiment_name,
            submission.id))
        #return self.make_control_nonaction(experiment_thing, submission, group="stub-treat")
        return self.make_sticky_post(experiment_thing, submission)


class StickyCommentMessagingExperimentController(StickyCommentExperimentController):
    def __init__(self, experiment_name, db_session, r, log):
        required_keys = ['subreddit', 'subreddit_id', 'username', 
                         'start_time', 'end_time', 'dry_run',
                         'conditions', 'non_study_messaging_enabled']
        super().__init__(experiment_name, db_session, r, log, required_keys)
        self.task_id = '{0}({1})'.format(self.__class__.__name__, experiment_name)
        self.log_prefix = '{0} Experiment {1}:'.format(self.__class__.__name__, experiment_name)
        self.non_study_messaging_enabled = self.experiment_settings['non_study_messaging_enabled']

    def archive_mod_action_user_things(self, mod_actions):
        self.log.info('%s Preparing to archive user experiment things from %s mod actions.',
            self.log_prefix, len(mod_actions))
        user_ids = set(mod_action.target_author for mod_action in mod_actions)
        archived_user_things = {user_thing.thing_id:user_thing
            for user_thing in self.fetch_user_things(user_ids)}
        archived_user_ids = set(user_thing.thing_id
            for user_thing in archived_user_things.values())
        unique_user_ids = user_ids - archived_user_ids
        user_things = {user_id:self.build_user_experiment_thing(user_id)
            for user_id in unique_user_ids}
        self.log.info('%s Archiving %s unique user experiment things.',
            self.log_prefix, len(user_things))
        if user_things:
            self.db_session.add_retryable(user_things.values())
        user_things.update(archived_user_things)
        return user_things
    
    def build_excluded_message_experiment_action(self, mod_action_thing, user_thing):
        experiment_action = ExperimentAction(
            action = 'SendExcludedMessage',
            experiment_id = self.experiment.id,
            action_subject_type = ThingType.MODACTION.value,
            action_subject_id = mod_action_thing.id,
            action_object_type = ThingType.USER.value,
            action_object_id = user_thing.thing_id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name)
        )
        action_metadata = {
            'condition': None,
            'arm': None,
            'randomization': None,
            'group': 'excluded'
        }
        # The message_data dictionary is the input expected by the messaging controller
        action_metadata['message_data'] = {
            'account': user_thing.thing_id,
            'subject': self.experiment_settings['standard_pm_subject'],
            'message': self.experiment_settings['standard_pm_text'].format(
                username=user_thing.thing_id)
        }
        experiment_action.metadata_json = json.dumps(action_metadata)
        return experiment_action
        
    def build_message_experiment_action(self, mod_action_thing, user_thing, group):
        mod_action_metadata = json.loads(mod_action_thing.metadata_json)
        randomizable = bool(mod_action_metadata['randomizable'])
        if randomizable:
            randomization = mod_action_metadata['randomization']
            condition = mod_action_metadata['condition']
            arm = 'arm_' + randomization['treatment']
            arm_config = self.experiment_settings['conditions'][condition]['arms'][arm]
        else:
            submission_id = mod_action_metadata['submission_id']
            post_thing = self.post_things[submission_id]
            post_metadata = json.loads(post_thing.metadata_json)
            randomization = post_metadata['randomization']
            condition = post_metadata['condition']
            arm = 'arm_' + randomization['treatment']
            arm_config = self.experiment_settings['conditions'][condition]['arms'][arm]
             
        log_prefix = '{0} condition {1} {2}:'.format(self.log_prefix, condition, arm)
        
        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            action_subject_type = ThingType.MODACTION.value,
            action_subject_id = mod_action_thing.id,
            action_object_type = ThingType.USER.value,
            action_object_id = user_thing.thing_id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name)
        )
        action_metadata = {
            'condition': condition,
            'arm': arm,
            'randomization': randomization,
            'group': group
        }
        
        # Record a control non-action if messaging was deliberately disabled
        # for this condition and arm by setting the max allowed pms to 0
        pm_max_count = arm_config['pm_max_count']
        if pm_max_count == 0:
            self.log.info('%s Control group with messaging disabled. No action taken.', log_prefix)
            action_metadata['action'] = 'ControlNoMessage'
            experiment_action.metadata_json = json.dumps(action_metadata)
            return experiment_action
        
        user_thing_metadata = json.loads(user_thing.metadata_json)
        post_id = mod_action_metadata['submission_id']
        post_message_count = user_thing_metadata['submission_message_counts'].setdefault(post_id, 0)
        if pm_max_count is not None and post_message_count >= pm_max_count:
            self.log.info('%s Already sent %s of %s messages to user %s for this thread. No action taken.',
                log_prefix,
                post_message_count,
                pm_max_count,
                user_thing.thing_id
            )
            return None
        user_thing_metadata['submission_message_counts'][post_id] += 1
        user_thing.metadata_json = json.dumps(user_thing_metadata)

        # Fetch the actual group from the parent post if needed, i.e. for
        # mod actions related to posts that were not in the "within guestbook" arm
        if group == 'post':
            post_thing = self.post_things[post_id]
            group = self.get_post_group(post_thing)

        if group == 'control':
            action_name = 'SendStandardMessage'
            message = self.experiment_settings[arm_config['pm_text_key']].format(
                username = user_thing.thing_id
            )
        else:
            action_name = 'SendGuestbookMessage'
            guestbook_url = 'https://reddit.com/r/{subreddit}/comments/{post_id}/_/{comment_id}'.format(
                subreddit = self.experiment_settings['subreddit'],
                post_id = post_id,
                comment_id = self.sticky_comment_things[post_id]
            )
            message = self.experiment_settings[arm_config['pm_text_key']].format(
                username = user_thing.thing_id,
                guestbook_link = guestbook_url
            )

        # The message_data dictionary is the input expected by the messaging controller
        action_metadata['message_data'] = {
            'account': user_thing.thing_id,
            'subject': self.experiment_settings[arm_config['pm_subject_key']],
            'message': message
        }
        experiment_action.action = action_name
        experiment_action.metadata_json = json.dumps(action_metadata)
        return experiment_action

    # Overrides and extends the base method to build a mod action experiment thing
    def build_condition_experiment_thing(self, obj, thing_type, condition, randomizable, randomization):
        thing = super().build_condition_experiment_thing(obj, thing_type, condition, randomizable, randomization)
        if thing_type is ThingType.SUBMISSION:
            return thing
        elif thing_type is ThingType.MODACTION:
            metadata = json.loads(thing.metadata_json)
            metadata['target_author'] = obj.target_author
            metadata['submission_id'] = self.extract_post_id(obj)
            thing.metadata_json = json.dumps(metadata)
            return thing
        
    def build_user_experiment_thing(self, user_id):
        user_thing = ExperimentThing(
            id = uuid.uuid4().hex,
            thing_id = user_id,
            experiment_id = self.experiment.id,
            object_type = ThingType.USER.value,
            object_created = None,
            metadata_json = json.dumps({
                'post_randomizations': {},
                'submission_message_counts': {}})
        )
        return user_thing
    
    def extract_post_id(self, mod_action):
        link_segments = mod_action.json_dict['target_permalink'].split('/')
        return link_segments[4]
    
    def fetch_incomplete_interventions(self):
         return self.db_session.query(ExperimentThing).filter(
             ExperimentThing.experiment_id == self.experiment.id,
             ExperimentThing.object_type == ThingType.MODACTION,
             ExperimentThing.query_index == 'Intervention TBD'
         )
    
    def fetch_post_things(self, post_ids):
        return self.db_session.query(ExperimentThing).filter(
            ExperimentThing.object_type == ThingType.SUBMISSION.value,
            ExperimentThing.experiment_id == self.experiment.id,
            ExperimentThing.id.in_(post_ids)
        )
    
    def fetch_sticky_comment_things(self, post_ids):
        post_id_strings = ["\"submission_id\": \"%s\"" % post_id for post_id in post_ids]
        return self.db_session.query(ExperimentThing).filter(
            ExperimentThing.object_type == ThingType.COMMENT.value,
            ExperimentThing.experiment_id == self.experiment.id,
            or_(ExperimentThing.metadata_json.contains(post_id_str) for post_id_str in post_id_strings)
        )
    
    def fetch_user_things(self, user_ids):
        return self.db_session.query(ExperimentThing).filter(
            ExperimentThing.experiment_id == self.experiment.id,
            ExperimentThing.object_type == ThingType.USER.value,
            ExperimentThing.thing_id.in_(user_ids)
        )
    
    def get_randomization(self, obj, thing_type, label):
        if thing_type is not ThingType.MODACTION:
            return super().get_randomization(obj, thing_type, label)
        target_author = obj.json_dict['target_author']
        user_thing = self.user_things[target_author]
        user_thing_metadata = json.loads(user_thing.metadata_json)
        post_id = self.extract_post_id(obj)
        post_randomizations = user_thing_metadata['post_randomizations']
        randomization = post_randomizations.get(post_id)
        if not randomization:
            randomization = super().get_randomization(obj, thing_type, label)
            # TODO Remove hardcoded "source condition" after the hack method just below
            # this method has been refactored and removed
            randomization['source_condition'] = 'ama_nonquestion_mod_action'
            post_randomizations[post_id] = randomization
            user_thing.metadata_json = json.dumps(user_thing_metadata)
        return randomization
    
    # This is a hack that needs to be refactored (but also working and tested). There should
    # needs to be an opposing function to get_randomization() that handles situations where
    # randomizable==False. This function is serving that purpose for now.
    def get_randomization_for_mod_action_default_arm(self, mod_action_thing, mod_action):
        target_author = mod_action.json_dict['target_author']
        user_thing = self.user_things[target_author]
        user_thing_metadata = json.loads(user_thing.metadata_json)
        post_id = self.extract_post_id(mod_action)
        post_randomizations = user_thing_metadata['post_randomizations']
        randomization = post_randomizations.get(post_id)
        if not randomization:
            post_thing = self.post_things[post_id]
            post_thing_metadata = json.loads(post_thing.metadata_json)
            randomization = post_thing_metadata['randomization']
            randomization['source_condition'] = 'ama_post'
            post_randomizations[post_id] = randomization
            user_thing_metadata['condition'] = 'ama_post'
            user_thing.metadata_json = json.dumps(user_thing_metadata)
        mod_action_thing_metadata = json.loads(mod_action_thing.metadata_json)
        mod_action_thing_metadata['randomizable'] = False # i.e. replace the null
        mod_action_thing_metadata['randomization'] = randomization
        mod_action_thing.metadata_json = json.dumps(mod_action_thing_metadata)
    
    def get_post_group(self, post_thing):
        metadata_json = json.loads(post_thing.metadata_json)
        treatment = int(metadata_json['randomization']['treatment'])
        if treatment == 0:
            return 'control'
        elif treatment == 1:
            return 'full_guestbook'
        elif treatment == 2:
            return 'within_guestbook'

    def identify_ama_post(self, submission):
        # This is accurate for r/iama as of 2020-05
        if 'selftext' not in submission.json_dict:
            return False
        locked = submission.json_dict.get('locked')
        author_flair_text = submission.json_dict.get('author_flair_text') or ''
        return not locked and 'crown_modgreen' not in author_flair_text

    def identify_ama_nonquestion_mod_action(self, mod_action):
        # This is accurate for r/iama as of 2020-05
        if 'mod' not in mod_action.json_dict:
            return False
        if not self.is_automod_comment_removal(mod_action):
            return False
        return mod_action.details == 'Not question'
    
    def intervene_ama_post_arm_0(self, thing, submission):
        return self.make_control_nonaction(thing, submission,
            group='control',
            action='ControlNoStickyPost')

    def intervene_ama_post_arm_1(self, thing, submission):
        return self.make_sticky_post(thing, submission,
            group='full_guestbook',
            action='MakeStickyPost')

    def intervene_ama_post_arm_2(self, thing, submission):
        return self.make_sticky_post(thing, submission,
            group='within_guestbook',
            action='MakeStickyPost')

    def intervene_ama_nonquestion_mod_action_arm_0(self, mod_action_thing, mod_action):
        return self.send_experiment_message(mod_action_thing, group='control')

    def intervene_ama_nonquestion_mod_action_arm_1(self, mod_action_thing, mod_action):
        return self.send_experiment_message(mod_action_thing, group='treatment')
    
    def intervene_ama_nonquestion_mod_action_default(self, mod_action_thing, mod_action):
        self.get_randomization_for_mod_action_default_arm(mod_action_thing, mod_action)
        return self.send_experiment_message(mod_action_thing, group='post')
    
    def is_automod_comment_removal(self, mod_action):
        return self.is_comment_removal(mod_action, mod_name='AutoModerator')
    
    def is_comment_removal(self, mod_action, mod_name=None):
        # This is accurate for r/iama as of 2020-05
        if 'mod' not in mod_action.json_dict:
            return False
        mod = mod_action.json_dict['mod']
        action = mod_action.json_dict['action']
        target_author = mod_action.json_dict['target_author']
        return (
            (not mod_name or mod == mod_name)
            and action == 'removecomment'
            and target_author != '[deleted]')
    
    def randomizable(self, obj, thing_type):
        if thing_type is not ThingType.MODACTION:
            return True
        post_thing = self.post_things[self.extract_post_id(obj)]
        post_metadata = json.loads(post_thing.metadata_json)
        post_condition = post_metadata['condition']
        post_arm = 'arm_' + post_metadata['randomization']['treatment']
        post_arm_config = self.experiment_settings['conditions'][post_condition]['arms'][post_arm]
        return post_arm_config.get('randomize_dependents')
    
    def send_excluded_message(self, mod_action):
        mod_action_thing = self.build_condition_experiment_thing(mod_action, ThingType.MODACTION, None, None, False)
        target_author = json.loads(mod_action_thing.metadata_json)['target_author']
        user_thing = self.user_things[target_author]
        message_action = self.build_excluded_message_experiment_action(mod_action_thing, user_thing)
        response = self.send_message(mod_action_thing, message_action)
        self.db_session.add_retryable([mod_action_thing, message_action])
        return response

    def send_experiment_message(self, mod_action_thing, group):
        target_author = json.loads(mod_action_thing.metadata_json)['target_author']
        user_thing = self.user_things[target_author]
        message_action = self.build_message_experiment_action(
            mod_action_thing, user_thing, group)
        response = None
        if message_action.action in ('SendStandardMessage', 'SendGuestbookMessage'):
            response = self.send_message(mod_action_thing, message_action)
        self.db_session.add_retryable(message_action)
        return response
    
    def send_message(self, mod_action_thing, message_action):
        from app.controllers.messaging_controller import MessagingController
        mc = MessagingController(self.db_session, self.r, self.log)
        message_metadata = json.loads(message_action.metadata_json)
        message = message_metadata['message_data']

        if self.dry_run:
            # Reproduce the log entry and result returned in mc.send_message()
            # for the sake of the dry run
            self.log.info("(DRY RUN) Sending a message to user %s with subject %s" % (
                message['account'],
                message['subject']))
            response = {'errors': []}
        else:
            response = mc.send_message(
                message['account'],
                message['message'],
                message['subject'],
                '%s::send_message' % self.task_id
            )
        
        errors = [e['error'] for e in response.get('errors', [])]
        if errors:
            if 'invalid username' in errors:
                message_metadata['message_status'] = 'nonexistent'
                mod_action_thing.query_index = 'Intervention Impossible'
            else:
                message_metadata['message_status'] = 'failed'
            message_metadata['errors'] = response['errors']
        else:
            message_metadata['message_status'] = 'sent'
            mod_action_thing.query_index = 'Intervention Complete'
        
        message_action.metadata_json = json.dumps(message_metadata)
        return response
    
    def update_experiment(self):
        """Override the base update_experiment method to disable it."""
        pass
       
    def update_experiment_mod_actions(self, instance):
        """Update experiment mod actions as a callback from ModeratorController.archive_mod_action_page()."""
        try:
            if instance.fetched_subreddit_id != self.experiment_settings['subreddit_id']:
                self.log.info('%s Callback update_experiment_mod_actions called but not needed for subreddit %s.',
                    self.log_prefix,
                    instance.fetched_subreddit_id)
                return
            self.log.info('%s Running callback update_experiment_mod_actions for subreddit %s.',
                self.log_prefix,
                instance.fetched_subreddit_id)
            lock_id = '%s::update_experiment_mod_actions' % self.task_id
            with self.db_session.cooplock(lock_id, self.experiment.id):
                eligible_mod_actions = self.get_eligible_objects(instance.fetched_mod_actions, ThingType.MODACTION)
                comment_removal_mod_actions = [ma for ma in eligible_mod_actions if self.is_comment_removal(ma)]
                experiment_mod_actions = [ma for ma in comment_removal_mod_actions
                    if self.identify_ama_nonquestion_mod_action(ma)]
                
                post_ids = [self.extract_post_id(mod_action) for mod_action in experiment_mod_actions]
                self.post_things = {}
                for post_thing in self.fetch_post_things(post_ids):
                    self.post_things[post_thing.id] = post_thing
                
                self.sticky_comment_things = {}
                for sticky_comment_thing in self.fetch_sticky_comment_things(post_ids):
                    post_id = json.loads(sticky_comment_thing.metadata_json)['submission_id']
                    self.sticky_comment_things[post_id] = sticky_comment_thing

                ready_experiment_mod_actions = [ma for ma in experiment_mod_actions
                    if self.extract_post_id(ma) in self.post_things]
                self.log.info('%s %d of %d eligible mod actions have a parent post included in the experiment.',
                    self.log_prefix,
                    len(ready_experiment_mod_actions),
                    len(eligible_mod_actions))
                
                # TODO Fix group parameter for incomplete_interventions
                #incomplete_interventions = self.fetch_incomplete_interventions()
                self.user_things = self.archive_mod_action_user_things(ready_experiment_mod_actions)
                results = self.run_interventions(ready_experiment_mod_actions, ThingType.MODACTION)
                #for mod_action_thing in incomplete_interventions:
                #    results.append(self.send_experiment_message(mod_action_thing))
                
                # Handle sending private messages for comment removals not included in the study
                if self.non_study_messaging_enabled:
                    excluded_comment_removal_mod_actions = [ma for ma in comment_removal_mod_actions if
                        self.is_automod_comment_removal(ma)
                        and not self.identify_ama_nonquestion_mod_action(ma)]
                    self.user_things.update(self.archive_mod_action_user_things(
                        excluded_comment_removal_mod_actions))
                    for mod_action in excluded_comment_removal_mod_actions:
                        results.append(self.send_excluded_message(mod_action))
                
                self.db_session.commit()
                return results
        except:
            self.log.exception('%s encountered an exception while running update_experiment_mod_actions.', self.log_prefix)
            self.db_session.rollback()
    
    def update_experiment_posts(self, instance):
        """Update experiment posts as a callback from SubredditPageController.archive_subreddit_page()."""
        try:
            if instance.fetched_subreddit_id != self.experiment_settings['subreddit_id']:
                self.log.info('%s Callback update_experiment_posts called but not needed for subreddit %s.',
                    self.log_prefix,
                    instance.fetched_subreddit_id)
                return
            self.log.info('%s Running callback update_experiment_posts for subreddit %s.',
                self.log_prefix,
                instance.fetched_subreddit_id)
            lock_id = '%s::update_experiment_posts' % self.task_id
            with self.db_session.cooplock(lock_id, self.experiment.id):
                eligible_objects = self.get_eligible_objects(instance.fetched_posts, ThingType.SUBMISSION)
                results = self.run_interventions(eligible_objects, ThingType.SUBMISSION)
                self.db_session.commit()
                return results
        except:
            self.log.exception('%s encountered an exception while running update_experiment_posts.', self.log_prefix)
            self.db_session.rollback()
