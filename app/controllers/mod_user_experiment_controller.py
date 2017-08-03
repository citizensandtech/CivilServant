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
from app.models import Base, ModAction, PrawKey
from app.models import Experiment, ExperimentThing, ExperimentAction, ExperimentThingSnapshot
from app.models import EventHook, UserMetadata, User
from sqlalchemy import and_, or_
from app.controllers.moderator_controller import ModeratorController
from app.controllers.experiment_controller import ExperimentController
import numpy as np

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..","..")
ENV = os.environ['CS_ENV']


BAN_USER_STR = "banuser"

class ModeratorExperimentController(ExperimentController):
    
    def __init__(self, experiment_name, db_session, r, log):
        required_keys = ['subreddit', 'subreddit_id', 
                         'shadow_subreddit', 'shadow_subreddit_id', 'username', 
                         'start_time', 'end_time',
                         #'max_eligibility_age', 'min_eligibility_age',
                         'conditions', 'event_hooks']

        super().__init__(experiment_name, db_session, r, log, required_keys)

    #########################################

    def pre(self):
        # get main subreddit's modlog history up til oldest_mod_action_created_utc, store UserMetadata records
        self.query_and_archive_banned_users_main() ####

    def query_and_archive_banned_users_main(self, oldest_mod_action_created_utc=None):
        # first make sure mod action history is archived
        if not hasattr(self, 'main_mod'):
            self.main_mod = ModeratorController(self.subreddit, self.db_session, self.r, self.log)

        self.main_mod.archive_mod_action_history()

        banned_users = self.get_banned_users(self.subreddit_id, oldest_mod_action_created_utc)
        existing_banned_user_id_to_usermetadata = self.get_existing_banned_user_id_to_usermetadata(self.subreddit_id, banned_users)
        banned_user_id_to_usermetadata = self.update_and_archive_user_metadata(self.subreddit_id, banned_users, existing_banned_user_id_to_usermetadata)

    # from ModActions since oldest_mod_action_created_utc, find banned users 
    # returns list of usernames ["xxxx"]
    def get_banned_users(self, subreddit_id, oldest_mod_action_created_utc):
        # correct?????
        sub_query = self.db_session.query(ModAction.target_author).filter(
                ModAction.subreddit_id == subreddit_id).filter(
                ModAction.action == BAN_USER_STR)
        if oldest_mod_action_created_utc:
            users = sub_query.filter(
                ModAction.created_utc >= oldest_mod_action_created_utc).all()
        else:
            users = sub_query.all()
        return [x[0] for x in users]


    # returns dict existing_banned_user_id_to_usermetadata
    def get_existing_banned_user_id_to_usermetadata(self, subreddit_id, banned_users):
        existing_banned_user_id_to_usermetadata = {}
        if len(banned_users) > 0:
            existing_banned_usermetadata = self.db_session.query(UserMetadata).filter(
                and_(
                    UserMetadata.field_name == UserMetadataField.NUM_PREVIOUS_BANS.name,
                    UserMetadata.subreddit_id == subreddit_id
                    )).filter(
                UserMetadata.user_id.in_(banned_users)).all()
            existing_banned_user_id_to_usermetadata = {umd.user_id: umd for umd in existing_banned_usermetadata}
        return existing_banned_user_id_to_usermetadata

    def update_and_archive_user_metadata(self, subreddit_id, banned_users, banned_user_id_to_usermetadata):
        self.log.info("banned_users: {0}".format(banned_users))
        for name in banned_users:
            if name in banned_user_id_to_usermetadata:
                user_ban_metadata = banned_user_id_to_usermetadata[name]
                user_ban_metadata.updated_at = datetime.datetime.utcnow()
                user_ban_metadata.field_value = str(int(user_ban_metadata.field_value) + 1) 
                self.db_session.commit()
            else:
                user_ban_metadata = UserMetadata(
                    user_id = name,
                    subreddit_id = subreddit_id,
                    created_at = datetime.datetime.utcnow(),
                    updated_at = datetime.datetime.utcnow(),
                    field_name = UserMetadataField.NUM_PREVIOUS_BANS.name,
                    field_value = "1")
                banned_user_id_to_usermetadata[name] = user_ban_metadata
                self.db_session.add(user_ban_metadata)

            self.log.info("update_and_archive_user_metadata: name={0}, banned_user_id_to_usermetadata={1}".format(
                name, {name: banned_user_id_to_usermetadata[name].field_value for name in banned_user_id_to_usermetadata}))
        self.db_session.commit()
        return banned_user_id_to_usermetadata

    #########################################


    def query_and_archive_new_banned_users_main(self):
        last_action = self.db_session.query(ModAction).filter(
            ModAction.subreddit_id == self.subreddit_id).order_by(
            ModAction.created_at.desc()).first() # latest action
        last_action_id = last_action.id if last_action else None
        last_mod_action_created_utc = last_action.created_utc if last_action else None

        self.query_and_archive_banned_users_main(last_mod_action_created_utc)

    def get_eligible_users_and_archive_mod_actions(self, instance):
        # get banned users from mod actions on shadow subreddit
        # {username_str: praw.objects.ModAction}
        # need to preserve multiple mod actions on same user
        all_banned_users = [action.target_author for action in instance.mod_actions if action.action == BAN_USER_STR]
        banned_user_id_to_usermetadata_main = self.get_existing_banned_user_id_to_usermetadata(self.shadow_subreddit_id, all_banned_users)

        # update user metadata records for shadow subreddit
        banned_user_id_to_usermetadata_shadow = self.update_and_archive_user_metadata(
            self.shadow_subreddit_id, all_banned_users, 
            self.get_existing_banned_user_id_to_usermetadata(self.subreddit_id, all_banned_users)
            ) # stores UserMetadata records

        # get {username: modaction} dict from this observation period's mod_actions instance.mod_actions
        banned_users_shadow_dict = {action.target_author: action for action in instance.mod_actions if 
            action.action == BAN_USER_STR}

        self.log.info("all_banned_users: {0}".format(all_banned_users))        
        self.log.info("banned_user_id_to_usermetadata_main: {0}".format(banned_user_id_to_usermetadata_main))        
        self.log.info("banned_users_shadow_dict: {0}".format(banned_users_shadow_dict))        

        # get users that already have ExperimentThing records
        already_processed_ids = []
        if len(all_banned_users) > 0:
            already_processed_ids = [thing.id for thing in 
                self.db_session.query(ExperimentThing).filter(and_(
                    ExperimentThing.object_type==ThingType.USER.value, 
                    ExperimentThing.experiment_id == self.experiment.id,
                    ExperimentThing.id.in_(all_banned_users))).all()]

        # get {username: modaction} dict for only first time banned
        # for every username mentioned in this observation period's list of modactions, it is newly banned if 
        # user has no modactions in main subreddit, and
        # user has only 1 modaction in shadow subreddit (from this observation period)
        newly_banned_users_dict = {u: banned_users_shadow_dict[u] for u in banned_users_shadow_dict if 
            u not in banned_user_id_to_usermetadata_main and 
            banned_user_id_to_usermetadata_shadow[u].field_value==str(1) and
            u not in already_processed_ids}


        self.log.info("already_processed_ids: {0}".format(already_processed_ids))        
        self.log.info("newly_banned_users_dict: {0}".format(newly_banned_users_dict))                                

        self.log.info("{0}: Experiment {1} Discovered {2} eligible users: {3}".format(
            self.__class__.__name__,
            self.experiment_name,
            len(newly_banned_users_dict),
            newly_banned_users_dict))



        # {username: praw.objects.ModAction}
        return newly_banned_users_dict

    # archives user objects 
    def archive_user_records(self, banned_username_to_modaction):
        existing_user_names = set([])
        if len(banned_username_to_modaction) > 0:
            existing_user_names = {user.name: user for user in self.db_session.query(User).filter(
                User.name.in_(banned_username_to_modaction))}

        # list of userids
        to_archive_user_names = [name for name in banned_username_to_modaction if name not in existing_user_names]

        seen_at = datetime.datetime.utcnow()

        banned_user_to_modaction = {}
        for user_name in to_archive_user_names:
            # query reddit
            praw_user = self.r.get_redditor(user_name)
            new_user = User(
                    name = user_name,
                    id = praw_user.id, # without "t2_"
                    created = datetime.datetime.fromtimestamp(praw_user.created_utc),
                    first_seen = seen_at,
                    last_seen = seen_at, 
                    user_data = json.dumps(praw_user.json_dict))
            self.db_session.add(new_user)
            banned_user_to_modaction[new_user] = banned_username_to_modaction[user_name]
        self.db_session.commit()

        for user_name in existing_user_names:
            user = existing_user_names[user_name]
            if seen_at > user.last_seen:
                user.last_seen = seen_at
            banned_user_to_modaction[user] = banned_username_to_modaction[user_name]
        self.db_session.commit()    

        return banned_user_to_modaction


    ### 4) ModUserExperimentController.update_experiment(instance)
    ### job: look for mod actions on shadow subreddit
    ###      event hook on shadow subreddit look ing for mod actions
    ###      upon finding a banned user on shadow subreddit, look for all mod actions on main subreddit
    def update_experiment(self, instance):
        self.log.info("IN UPDATE_EXPERIMENT; instance.subreddit_name={0}, self.shadow_subreddit={1}, instance.mod_actions={2}".format(
            instance.subreddit_name,
            self.shadow_subreddit,
            instance.mod_actions
            ))
        # make sure to only run this callback only if the ModeratorController instance 
        # is fetching mod actions for this experiment's shadow subreddit 
        if instance.subreddit_name != self.shadow_subreddit or len(instance.mod_actions) == 0:    
            return

        # get and store newest mod actions on main subreddit
        #################self.query_and_archive_new_banned_users_main()

        # get new mod actions from shadow subreddit
        banned_username_to_modaction = self.get_eligible_users_and_archive_mod_actions(instance)

        self.log.info("IN UPDATE_EXPERIMENT; banned_username_to_modaction={0}".format(
            banned_username_to_modaction
            ))

        # store User records
        banned_user_to_modaction = self.archive_user_records(banned_username_to_modaction)

        if len(banned_username_to_modaction) > 0:
            return self.run_interventions(banned_user_to_modaction)


    # takes in {username: mod action detail}
    # otherwise same as sticky_comment_experiment_controller
    def run_interventions(self, banned_user_to_modaction):
        rvals = []
        for experiment_thing in self.assign_randomized_conditions(banned_user_to_modaction): #####
            condition = json.loads(experiment_thing.metadata_json)['condition']
            randomization = json.loads(experiment_thing.metadata_json)['randomization']
            arm_label = "arm_"+str(randomization['treatment'])
            intervene = getattr(self, "intervene_" + condition + "_" + arm_label)
            rval = intervene(experiment_thing)
 
            if(rval is not None):
                rvals.append(rval)
        return rvals


    def assign_randomized_conditions(self, banned_user_to_modaction):
        if(banned_user_to_modaction is None or len(banned_user_to_modaction)==0):
            return []
        ## Assign experiment condition to objects
        experiment_things = []
        for user in banned_user_to_modaction:
            label = self.identify_condition(user)
            if(label is None):
                continue

            no_randomizations_remain = False

            condition = self.experiment_settings['conditions'][label]
            try:
                randomization = condition['randomizations'][condition['next_randomization']]
                self.experiment_settings['conditions'][label]['next_randomization'] += 1
            except:
                self.log.error("{0}: Experiment {1} has used its full stock of {2} {3} conditions. Cannot assign any further.".format(
                    self.__class__.__name__,
                    self.experiment.name,
                    len(condition['randomizations']),
                    label
                ))
                no_randomizations_remain = True

            if(no_randomizations_remain):
                continue
            experiment_thing = ExperimentThing(
                id             = user.name,
                object_type    = ThingType.USER.value,
                experiment_id  = self.experiment.id,
                object_created = user.created,
                metadata_json  = json.dumps({
                    "randomization":randomization, 
                    "condition":label, 
                    "modaction":banned_user_to_modaction[user].json_dict
                    })
            )
            self.db_session.add(experiment_thing)
            experiment_things.append(experiment_thing)
            
        self.experiment.settings_json = json.dumps(self.experiment_settings)
        self.db_session.commit()
        self.log.info("{0}: Experiment {1}: assigned conditions to {2} usernames".format(
            self.__class__.__name__, self.experiment.name,len(experiment_things)))
        return experiment_things


    ## Check the acceptability of a user before acting
    def user_acceptable(self, username):
        if(username is None):
            ## TODO: Determine what to do if you can't find the user
            self.log.error("{0}: Can't find experiment {1} user in subreddit {2}".format(
                self.__class__.__name__, self.experiment_name, self.subreddit))
            return False

        ## Avoid Acting if the Intervention has already been recorded
        if(self.db_session.query(ExperimentAction).filter(and_(
            ExperimentAction.experiment_id      == self.experiment.id,
            ExperimentAction.action_object_type == ThingType.USER.value,
            ExperimentAction.action_object_id   == username,
            ExperimentAction.action             == "Intervention")).count() > 0):
                self.log.info("{0}: Experiment {1} user {2} already has an Intervention recorded".format(
                    self.__class__.__name__,
                    self.experiment_name, 
                    username))            
                return False

        # check no UserMetadata for them

        return True

    # get username from experiment_thing.id
    # only need modaction!=None if group="control"
    def apply_ban(self, experiment_thing, group="control", duration=None):
        username = experiment_thing.id
        if(self.user_acceptable(username) == False):
            return None

        metadata = json.loads(experiment_thing.metadata_json)
        treatment_arm = int(metadata['randomization']['treatment'])
        condition     = metadata['condition']
        modaction = metadata["modaction"]

        #####################
        message_text = ""
        exp_action_metadata = {
            "group":group, 
            "condition":condition,
            "arm":"arm_" + str(treatment_arm)
        }

        self.log.info("modaction={0}".format(modaction))
        if group == "treatment":
            reason_text = "treatment reason text"
            message_text  = "treatment message text" # self.experiment_settings['conditions'][condition]['arms']["arm_" + str(treatment_arm)]
            exp_action_metadata["randomization"] = metadata['randomization']
            duration = duration ########

        elif group == "control":
            reason_text = "control reason text" # modaction["description"]    #############         
            message_text  = "control message text"
            try:
                duration = self.parse_days_from_details(modaction["details"])
            except:
                self.log.error("{0}: Experiment {1} failed to make_control_nonaction on experiment_thing id {2}. error={3}".format(
                    self.__class__.__name__,
                    self.experiment_name,
                    experiment_thing.id,
                    sys.exc_info()[0]))
                return

        self.log.info("reason_text={0}".format(reason_text))
        self.log.info("duration={0}".format(duration))        

        # add ban
        if not hasattr(self, "main_sub"):
            self.main_sub = self.r.get_subreddit(self.subreddit)        

        try:
            if not duration: #perma ban
                self.main_sub.add_ban(username, ban_reason=reason_text, ban_message=message_text)
            else: # temp ban
                self.main_sub.add_ban(username, ban_reason=reason_text, ban_message=message_text, duration=duration)
        except:
            self.log.error("{0}: Experiment {1} failed to add ban on experiment_thing id {2}, username={3}, description={4}, duration={5}, error={6}".format(
                self.__class__.__name__,
                self.experiment_name,
                experiment_thing.id,
                username,
                message_text,
                duration,
                sys.exc_info()[0]))
            return


        # add ExperimentAction
        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action_subject_type = ThingType.USER.value,
            action_subject_id = modaction["mod"], ########
            action = "Intervention",
            action_object_type = ThingType.USER.value,
            action_object_id = username,
            metadata_json = json.dumps(exp_action_metadata))
        self.db_session.add(experiment_action)
        self.db_session.commit()

        self.log.info("{0}: Experiment {1} applied group {2}, condition {3}, treatment_arm {4} to username {5}".format(
            self.__class__.__name__,
            self.experiment_name, 
            group,
            condition,
            treatment_arm,
            username
        ))

        return experiment_action.id

    def make_control_nonaction(self, experiment_thing, group="control"):
        self.apply_ban(experiment_thing, group=group)

    def apply_temp_ban(self, experiment_thing, group="treatment", duration=7):
        self.apply_ban(experiment_thing, group=group, duration=duration)

    def apply_perma_ban(self, experiment_thing, group="treatment"):
        self.apply_ban(experiment_thing, group=group, duration=None)

    def parse_days_from_details(self, details_str):
        duration = None # if None, then permanent
        parse_error = False
        if details_str == "permanent":
            duration = None
        else:
            details_list = details_str.split(" ")
            if details_list[1] != "days":
                parse_error = True

            try:
                duration = int(details_list[0])
            except:
                parse_error = True

        if not parse_error:
            return duration
        else:          
            self.log.error("{0}: Experiment {1} error while parsing details to apply control nonaction to user {2}. details={3}. exception={4}".format(
                self.__class__.__name__,
                self.experiment_name,
                username,
                details_str,
                sys.exc_info()[0]))
            raise Exception ##########

class ModUserExperimentController(ModeratorExperimentController):
    
    def __init__(self, experiment_name, db_session, r, log):
        super().__init__(experiment_name, db_session, r, log)

    # called by identify_condition
    def identify_main(self, username):
        # only 1 condition, so all return True
        return True

    ## CONTROL GROUP
    def intervene_main_arm_0(self, experiment_thing):
        self.log.info(">>>Attempting intervene_main_arm_0 control on user {0}, details={1}, description={2}".format(
            experiment_thing.id,
            json.loads(experiment_thing.metadata_json)["modaction"]["details"],
            json.loads(experiment_thing.metadata_json)["modaction"]["description"]
            ))
        return self.make_control_nonaction(experiment_thing, group="control")
        
    ## TREATMENT GROUP 1
    def intervene_main_arm_1(self, experiment_thing):
        self.log.info(">>>Attempting intervene_main_arm_1 perma ban on user {0}, details={1}, description={2}".format(
            experiment_thing.id,
            json.loads(experiment_thing.metadata_json)["modaction"]["details"],
            json.loads(experiment_thing.metadata_json)["modaction"]["description"]
            ))
        return self.apply_perma_ban(experiment_thing, group="treatment")

    ## TREATMENT GROUP 2
    def intervene_main_arm_2(self, experiment_thing):
        self.log.info(">>>Attempting intervene_main_arm_2 temp ban on user {0}, details={1}, description={2}".format(
            experiment_thing.id,
            json.loads(experiment_thing.metadata_json)["modaction"]["details"],
            json.loads(experiment_thing.metadata_json)["modaction"]["description"]
            ))
        return self.apply_temp_ban(experiment_thing, group="treatment", duration=7)