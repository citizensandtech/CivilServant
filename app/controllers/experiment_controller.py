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
from app.models import Base, SubredditPage, Subreddit, User, Post, ModAction, PrawKey, Comment
from app.models import Experiment, ExperimentThing, ExperimentAction, ExperimentThingSnapshot
from app.models import EventHook
from sqlalchemy import and_, or_

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']


class ExperimentConfigurationError(Exception):
    def __init__(self, message, errors = []):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        # Now for your custom code...
        #self.errors = errors


class ExperimentController():
    def __init__(self, experiment_name, db_session, r, log, required_keys):
        self.db_session = db_session
        self.log = log
        self.r = r
        self.required_keys = required_keys
        self.load_experiment_config(required_keys, experiment_name)
        self.log.info("Initializing experiment {0}".format(experiment_name))

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
            self.db_session.add(experiment)
            self.db_session.commit()
        
        ### SET UP INSTANCE PROPERTIES
        self.experiment = experiment
        self.experiment_settings = json.loads(self.experiment.settings_json)
        

        self.experiment_name = experiment_name
        self.dry_run = experiment_config.get("dry_run", False)

        for key in ['subreddit', 'subreddit_id', 'shadow_subreddit', 'shadow_subreddit_id', 
                    'username', 'max_eligibility_age', 'min_eligibility_age']:
            if key in required_keys:
                setattr(self, key, experiment_config[key])

        # LOAD EVENT HOOKS
        if 'event_hooks' in required_keys:
            self.load_event_hooks(experiment_config)

    def load_event_hooks(self, experiment_config):
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
                self.db_session.add(hook_record)
                self.db_session.commit()

    ###########################

    
    def identify_condition(self, obj):
        for label in self.experiment_settings['conditions'].keys():
            detection_method = getattr(self, "identify_"+label)
            if(detection_method(obj)):
                return label
        return None
