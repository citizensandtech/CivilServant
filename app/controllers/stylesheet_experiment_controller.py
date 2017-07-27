import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime, yaml, time, csv, pytz
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
import numpy as np

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']


class StylesheetExperimentController:
    def __init__(self, experiment_name, db_session, r, log, 
                       required_keys = ['subreddit', 'subreddit_id', 'username', 
                         'start_time', 'end_time', 'conditions', 
                         'intervention_interval_seconds', 'intervention_window_seconds']):
        self.db_session = db_session
        self.log = log
        self.r = r
        self.load_experiment_config(required_keys, experiment_name)
        
    def get_experiment_config(self, required_keys, experiment_name):        
        experiment_file_path = os.path.join(BASE_DIR, "config", "experiments", experiment_name) + ".yml"
        with open(experiment_file_path, 'r') as f:
            try:
                experiment_config_all = yaml.load(f)
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
            self.db_session.add(experiment)
            self.db_session.commit()
        
        ### SET UP INSTANCE PROPERTIES
        self.experiment = experiment
        self.experiment_settings = json.loads(self.experiment.settings_json)
        

        self.experiment_name = experiment_name

        self.subreddit = experiment_config['subreddit']
        self.subreddit_id = experiment_config['subreddit_id']
        self.username = experiment_config['username']

    def update_experiment(self):
        ## DETERMINE ELIGIBILITY
        if(self.determine_intervention_eligible()):
            condname = self.select_condition()
            self.run_intervention(condname)

    def determine_intervention_eligible(self):
        current_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        start_time = parser.parse(self.experiment_settings['start_time'])
        end_time = parser.parse(self.experiment_settings['end_time'])
        eligible = ((current_time > start_time) and (current_time < end_time))

        last_experiment_created = None

        if(eligible):
            last_experiment_action = self.db_session.query(ExperimentAction).filter(
                ExperimentAction.experiment_id==self.experiment.id).order_by(ExperimentAction.created_at).first()

            ## keep eligible True if there's no previous action
            ## if there is a previous action, check that it falls within
            ## the interval
            if(last_experiment_action):
                last_experiment_created = last_experiment_action.created_at.replace(tzinfo=pytz.utc)
                interval_since_last_action = (current_time - last_experiment_created).total_seconds()
                eligible = ((interval_since_last_action < self.experiment_settings['intervention_interval_seconds'] + self.experiment_settings['intervention_window_seconds']) and (interval_since_last_action >= self.experiment_settings['intervention_interval_seconds'] - self.experiment_settings['intervention_window_seconds']))
                # import pdb; pdb.set_trace()
                # pass
        if(eligible==False):
            self.log.info("{0}: Experiment {1} code run. Ineligible to Continue. Begin Time: {2}. End Time: {3}. Last Intervention: {4} ".format(
                self.__class__.__name__,
                self.experiment.name,
                start_time,
                end_time,
                last_experiment_created
            ))
        
        return eligible
    
    ### TWO CONDITIONS: NORMAL AND SPECIAL
    ### MONDAYS FRIDAYS SATURDAYS
    def select_condition(self, current_time=datetime.datetime.utcnow()):
        isoweekdays = {0:"Sunday",1:"Monday", 2:"Tuesday", 
            3:"Wednesday", 4:"Thursday", 5:"Friday", 6:"Saturday"}

        isoweekday = isoweekdays[current_time.date().isoweekday()]
        
        ## weekdays in the configuration file conditions should be exclusive
        for condition_name, values in self.experiment_settings['conditions'].items():
            if isoweekday in values['days'].split(","):
                return condition_name
        return None

    def run_intervention(self, condname):
        cond = self.experiment_settings['conditions'][condname]
        no_randomizations_remain = False

        try:
            randomization = cond['randomizations'][cond['next_randomization']]
            arm = "arm_" + randomization['treatment']
            self.experiment_settings['conditions'][condname]['next_randomization'] += 1
        except:
            #import pdb;pdb.set_trace
            self.log.error("{0}: Experiment {1} condition {2} has used its full stock of {3} {4} conditions. Cannot assign any further.".format(
                self.__class__.__name__,
                self.experiment.name,
                condname, 
                len(cond['randomizations']),
                arm
            ))
            no_randomizations_remain = True

        if(no_randomizations_remain):
            return False
        
        self.experiment.settings_json = json.dumps(self.experiment_settings)
        self.db_session.commit()
        self.log.info("{0}: Experiment {1}: assigned condition {2} arm {3}".format(
            self.__class__.__name__, self.experiment.name, condname, arm))

        intervene = getattr(self, "intervene_" + condname + "_" + arm)
        return intervene(condname)


    def set_stylesheet(self, condition, arm):
        arms = self.experiment_settings['conditions'][condition]['arms']
        intervention_line = arms[arm]

        found_code = False

        stylesheet_data = self.r.get_stylesheet(self.subreddit)
        if "stylesheet" in stylesheet_data.keys():
            line_list = []
            for line in stylesheet_data['stylesheet'].split("\n"):
                ## IF A LINE FROM THE STUDY IS FOUND,
                ## REPLACE IT WITH THE INTERVENTION
                if line in arms.values():
                    line = intervention_line
                    found_code = True
                line_list.append(line)

            ## IF THE CODE IS NOT FOUND, ADD IT TO THE END
            if(found_code!=True):
                line_list.append("/* CivilServantBot Experiment CSS */")
                line_list.append(intervention_line)
                line_list.append("")
            new_stylesheet = "\n".join(line_list)

            
        
        result = self.r.set_stylesheet(self.subreddit, new_stylesheet)
        if('errors' in result.keys() and len(result['errors'])==0):

            self.log.info("{0}: Experiment {1}: Applied Arm {2} of Condition {3}".format(
                    self.__class__.__name__,
                    self.experiment.id,
                    self.subreddit, 
                    arm,
                    condition))
            experiment_action = ExperimentAction(
                experiment_id = self.experiment.id,
                praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
                action = "Intervention:{0}.{1}".format(condition,arm),
                action_object_type = ThingType.STYLESHEET.value,
                action_object_id = None,
                metadata_json  = json.dumps({"arm":arm, "condition":condition})

            )
        else:
            self.log.error("{0}: Experiment {1}: Failed to apply Arm {2} of Condition {3}. Reddit errors: {4}".format(
                    self.__class__.__name__,
                    self.experiment.id,
                    self.subreddit, 
                    arm,condition, ", ".join(result['errors'])))
            experiment_action = ExperimentAction(
                experiment_id = self.experiment.id,
                praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
                action = "NonIntervention:PrawError.{0}.{1}".format(condition,arm),
                action_object_type = ThingType.STYLESHEET.value,
                action_object_id = None
            )
        self.db_session.add(experiment_action)
        self.db_session.commit()

        ## TO HELP WITH TESTING, RETURN THE FULL TEXT OF THE STYLESHEET
        return new_stylesheet

    def intervene_normal_arm_0(self, condname):
        return self.set_stylesheet(condname, "arm_0")

    def intervene_normal_arm_1(self, condname):
        return self.set_stylesheet(condname, "arm_1")

    def intervene_special_arm_0(self, condname):
        return self.set_stylesheet(condname, "arm_0")

    def intervene_special_arm_1(self, condname):
        return self.set_stylesheet(condname, "arm_1")

    def sample_comments(self):
        pass
    
    

        
