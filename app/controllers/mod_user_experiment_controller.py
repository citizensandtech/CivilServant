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

def parse_days_from_details(details_str):
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

class ModeratorExperimentController(ExperimentController):
    
    def __init__(self, experiment_name, db_session, r, log):
        required_keys = ['subreddit', 'subreddit_id', 
                         'shadow_subreddit', 'shadow_subreddit_id', 'username', 
                         'start_time', 'end_time',
                         'max_ban_duration',
                         #'max_eligibility_age', 'min_eligibility_age',
                         #### TODO: how to put in duration, reason_text?
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
    # returns list of ids ["xxxx"]
    def get_banned_users(self, subreddit_id, oldest_mod_action_created_utc):
        # correct?????
        sub_query = self.db_session.query(ModAction.target_fullname).filter(
                ModAction.subreddit_id == subreddit_id).filter(
                ModAction.action == BAN_USER_STR)
        if oldest_mod_action_created_utc:
            users = sub_query.filter(
                ModAction.created_utc >= oldest_mod_action_created_utc).all()
        else:
            users = sub_query.all()
        return [x[0].strip("t2_") for x in users] #ids


    # returns dict existing_banned_user_id_to_usermetadata
    def get_existing_banned_user_id_to_usermetadata(self, subreddit_id, banned_users):
        existing_banned_user_id_to_usermetadata = {}
        if len(banned_users) > 0:
            existing_banned_usermetadata = self.db_session.query(UserMetadata).filter(
                and_(UserMetadata.field_name == UserMetadataField.NUM_PREVIOUS_BANS.name,
                    UserMetadata.subreddit_id == subreddit_id)).filter(
                UserMetadata.user_id.in_(banned_users)).all() #user_id is actually username
            existing_banned_user_id_to_usermetadata = {umd.user_id: umd for umd in existing_banned_usermetadata}
        return existing_banned_user_id_to_usermetadata

    def update_and_archive_user_metadata(self, subreddit_id, banned_users, banned_user_id_to_usermetadata):
        ##self.log.info("banned_users: {0}".format(banned_users))
        for uid in banned_users:
            if uid in banned_user_id_to_usermetadata:
                user_ban_metadata = banned_user_id_to_usermetadata[uid]
                user_ban_metadata.updated_at = datetime.datetime.utcnow()
                user_ban_metadata.field_value = str(int(user_ban_metadata.field_value) + 1) 
                self.db_session.commit()
            else:
                user_ban_metadata = UserMetadata(
                    user_id = uid,
                    subreddit_id = subreddit_id,
                    created_at = datetime.datetime.utcnow(),
                    updated_at = datetime.datetime.utcnow(),
                    field_name = UserMetadataField.NUM_PREVIOUS_BANS.name,
                    field_value = "1")
                banned_user_id_to_usermetadata[uid] = user_ban_metadata
                self.db_session.add(user_ban_metadata)

            ##self.log.info("update_and_archive_user_metadata: name={0}, banned_user_id_to_usermetadata={1}".format(
            ##    name, {name: banned_user_id_to_usermetadata[name].field_value for name in banned_user_id_to_usermetadata}))
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
        # get banned users from mod actions on shadow subreddit, if mod actions within experiment time
        # {user id: praw.objects.ModAction}
        # need to preserve multiple mod actions on same user
        banactions = [action for action in instance.mod_actions if action.action == BAN_USER_STR]
        all_banned_users = [action.target_fullname.strip("t2_") for action in banactions if
            datetime.datetime.fromtimestamp(action.created_utc) >= self.experiment.start_time and 
            datetime.datetime.fromtimestamp(action.created_utc) <= self.experiment.end_time]
        banned_users_set = set(all_banned_users)
        banned_user_id_to_usermetadata_main = self.get_existing_banned_user_id_to_usermetadata(self.subreddit_id, all_banned_users)

        # update user metadata records for shadow subreddit
        banned_user_id_to_usermetadata_shadow = self.update_and_archive_user_metadata(
            self.shadow_subreddit_id, all_banned_users, 
            self.get_existing_banned_user_id_to_usermetadata(self.shadow_subreddit_id, all_banned_users)
            ) # stores UserMetadata records

        # get {user id: modaction} dict from this observation period's mod_actions instance.mod_actions
        banned_users_shadow_dict = {action.target_fullname.strip("t2_"): action for action in banactions if 
            action.target_fullname.strip("t2_") in banned_users_set}

        self.log.info("all_banned_users: {0}".format(all_banned_users))        
        self.log.info("banned_users_shadow_dict: {0}".format(banned_users_shadow_dict))        

        self.log.info("banned_user_id_to_usermetadata_main={0}".format(
            {uid: banned_user_id_to_usermetadata_main[uid].field_value for uid in banned_user_id_to_usermetadata_main}))
        self.log.info("banned_user_id_to_usermetadata_shadow={0}".format(
            {uid: banned_user_id_to_usermetadata_shadow[uid].field_value for uid in banned_user_id_to_usermetadata_shadow}))


        # get users that already have ExperimentThing records
        already_processed_user_ids = []
        if len(all_banned_users) > 0:
            already_processed_user_ids = [thing.id for thing in 
                self.db_session.query(ExperimentThing).filter(and_(
                    ExperimentThing.object_type==ThingType.USER.value, 
                    ExperimentThing.experiment_id == self.experiment.id,
                    ExperimentThing.id.in_(all_banned_users))).all()]

        # get {user id: modaction} dict for only first time banned
        # for every user id mentioned in this observation period's list of modactions, it is newly banned if 
        # user has no modactions in main subreddit, and
        # user has only 1 modaction in shadow subreddit (from this observation period)
        newly_banned_users_dict = {u: banned_users_shadow_dict[u] for u in banned_users_shadow_dict if 
            u not in banned_user_id_to_usermetadata_main and 
            banned_user_id_to_usermetadata_shadow[u].field_value==str(1) and
            u not in already_processed_user_ids}


        self.log.info("already_processed_user_ids: {0}".format(already_processed_user_ids))        
        self.log.info("newly_banned_users_dict: {0}".format(newly_banned_users_dict))                                

        self.log.info("{0}: Experiment {1} Discovered {2} eligible users: {3}".format(
            self.__class__.__name__,
            self.experiment_name,
            len(newly_banned_users_dict),
            newly_banned_users_dict))


        # {user id: praw.objects.ModAction}
        return newly_banned_users_dict

    # archives user objects 
    def archive_user_records(self, banned_user_id_to_modaction):
        existing_user_ids = set([])
        if len(banned_user_id_to_modaction) > 0:
            existing_user_ids = {user.id: user for user in self.db_session.query(User).filter(
                User.id.in_(banned_user_id_to_modaction))}

        # list of userids
        to_archive_user_ids = [uid for uid in banned_user_id_to_modaction if uid not in existing_user_ids]

        seen_at = datetime.datetime.utcnow()

        banned_user_to_modaction = {}
        for user_id in to_archive_user_ids:
            user_name = banned_user_id_to_modaction[user_id].target_author
            # query reddit
            try:
                praw_user = self.r.get_redditor(user_name) # this seems to always lazily evaluate?
                uid = praw_user.id # if user doesn't except, this line (which sends a get request) will throw an exception
                if uid != user_id:
                    # then somehow, queried id is different from user id stored in the mod log. 
                    # queried user is likely different from user stored in mod log.
                    raise Exception 
            except:
                # couldn't find user
                new_user = User(
                        name = user_name,
                        id = user_id,
                        created = None,
                        first_seen = seen_at,
                        last_seen = seen_at, 
                        user_data = None)                
            else:
                # found the user
                new_user = User(
                        name = user_name,
                        id = uid, # without "t2_"
                        created = datetime.datetime.fromtimestamp(praw_user.created_utc),
                        first_seen = seen_at,
                        last_seen = seen_at, 
                        user_data = json.dumps(praw_user.json_dict))
            self.db_session.add(new_user)
            banned_user_to_modaction[new_user] = banned_user_id_to_modaction[user_id]
        self.db_session.commit()

        for user_id in existing_user_ids:
            user = existing_user_ids[user_id]
            if seen_at > user.last_seen:
                user.last_seen = seen_at
            banned_user_to_modaction[user] = banned_user_id_to_modaction[user_id] # map User record to praw mod action object
        self.db_session.commit()    

        return banned_user_to_modaction


    ### 4) ModUserExperimentController.update_experiment(instance)
    ### job: look for mod actions on shadow subreddit
    ###      event hook on shadow subreddit look ing for mod actions
    ###      upon finding a banned user on shadow subreddit, look for all mod actions on main subreddit
    def update_experiment(self, instance):
        ##self.log.info("IN UPDATE_EXPERIMENT; instance.subreddit_name={0}, self.shadow_subreddit={1}, instance.mod_actions={2}".format(
        ##    instance.subreddit_name,
        ##    self.shadow_subreddit,
        ##    instance.mod_actions
        ##    ))
        
        # make sure to only run this callback only if the ModeratorController instance 
        # is fetching mod actions for this experiment's shadow subreddit 
        if instance.subreddit_name != self.shadow_subreddit or len(instance.mod_actions) == 0:    
            return

        # OTHER TASKS TO TRIGGER
        self.query_and_archive_new_banned_users_main() # get and store newest mod actions on main subreddit
        self.conclude_intervention() # conclude bans that have expired

        # get new mod actions from shadow subreddit
        banned_user_id_to_modaction = self.get_eligible_users_and_archive_mod_actions(instance)


        ##self.log.info("IN UPDATE_EXPERIMENT; banned_user_id_to_modaction={0}".format(banned_user_id_to_modaction))

        # store User records
        banned_user_to_modaction = self.archive_user_records(banned_user_id_to_modaction)

        if len(banned_user_id_to_modaction) > 0:
            return self.run_interventions(banned_user_to_modaction)


    def conclude_intervention(self):
        oldest_time = datetime.datetime.utcnow() - datetime.timedelta(days=self.experiment_settings['max_ban_duration'])
        # get all "Intervention" ExperimentAction.action_object_id where ExperimentAction.created_at > duration
        old_user_to_exaction = {ea.action_object_id: ea for ea in self.db_session.query(ExperimentAction).filter(
            and_(ExperimentAction.action == "Intervention",
                ExperimentAction.created_at <= oldest_time)).all()}

        # get all old_user_to_exaction that don't have a "ConcludeIntervention" ExperimentAction
        concluded_users = [ea.action_object_id for ea in self.db_session.query(ExperimentAction).filter(
            and_(ExperimentAction.action_object_id.in_(old_user_to_exaction),
                ExperimentAction.action == "ConcludeIntervention")).all()]

        if not hasattr(self, "main_sub"):
            self.main_sub = self.r.get_subreddit(self.subreddit)

        new_ea_ids = []
        for uid in old_user_to_exaction:
            metadata = json.loads(old_user_to_exaction[uid].metadata_json)
            username = metadata["shadow_modaction"]["target_author"]
            if uid not in concluded_users:
                try:
                    if metadata["shadow_modaction"]["details"] == "permanent":
                        self.main_sub.remove_ban(username)
                    # we don't have to do anything
                except:
                    self.log.error("{0}: Experiment {1} failed to conclude intervention on experiment_thing id {2}, username={3}, error={4}".format(
                        self.__class__.__name__,
                        self.experiment_name,
                        uid,
                        username,
                        sys.exc_info()[0]))
                else:
                    experiment_action = ExperimentAction(
                        experiment_id = self.experiment.id,
                        praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
                        action_subject_type = ThingType.USER.value,
                        action_subject_id = metadata["shadow_modaction"]["mod"], ########
                        action = "ConcludeIntervention",
                        action_object_type = ThingType.USER.value,
                        action_object_id = uid,
                        metadata_json = json.dumps({
                            "intervention_id": old_user_to_exaction[uid].id,
                            "shadow_modaction_id": metadata["shadow_modaction"]["id"], # original shadow mod action
                            "main_modaction_id": metadata["main_modaction_id"] # original shadow mod action
                        }
                    ))
                    self.db_session.add(experiment_action)
                    self.db_session.commit()
                    new_id = experiment_action.id
                new_ea_ids.append(new_id)

        self.log.info("{0}: Experiment {1}: concluded interventions to {2} users".format(
            self.__class__.__name__, self.experiment.name, len(new_ea_ids)))
        return new_ea_ids

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
                id             = user.id,
                object_type    = ThingType.USER.value,
                experiment_id  = self.experiment.id,
                object_created = user.created,
                metadata_json  = json.dumps({
                    "randomization":randomization, 
                    "condition":label, 
                    "shadow_modaction":banned_user_to_modaction[user].json_dict
                    })
            )
            self.db_session.add(experiment_thing)
            experiment_things.append(experiment_thing)
            
        self.experiment.settings_json = json.dumps(self.experiment_settings)
        self.db_session.commit()
        self.log.info("{0}: Experiment {1}: assigned conditions to {2} usernames".format(
            self.__class__.__name__, self.experiment.name, len(experiment_things)))
        return experiment_things


    ## Check the acceptability of a user before acting
    def user_acceptable(self, user_id):
        if(user_id is None):
            ## TODO: Determine what to do if you can't find the user
            self.log.error("{0}: Can't find experiment {1} user in subreddit {2}".format(
                self.__class__.__name__, self.experiment_name, self.subreddit))
            return False

        ## Avoid Acting if the Intervention has already been recorded
        if(self.db_session.query(ExperimentAction).filter(and_(
            ExperimentAction.experiment_id      == self.experiment.id,
            ExperimentAction.action_object_type == ThingType.USER.value,
            ExperimentAction.action_object_id   == user_id,
            ExperimentAction.action             == "Intervention")).count() > 0):
                self.log.info("{0}: Experiment {1} user {2} already has an Intervention recorded".format(
                    self.__class__.__name__,
                    self.experiment_name, 
                    user_id))            
                return False

        # check no UserMetadata for them

        return True

    def find_latest_mod_action_id_with(self, mod_action_query):
        # grab mod log of main subreddit, find id of mod action taken on username, put modaction id in metadata json
        if not hasattr(self, 'main_mod'):
            self.main_mod = ModeratorController(self.subreddit, self.db_session, self.r, self.log)
        main_mod_action_id = self.main_mod.find_latest_mod_action_id_with(mod_action_query)
        return main_mod_action_id

    # this method is not done / tested
    def auto_reply(self):
        ban_messages = []
        ban_subjects = []
        for arm in self.experiment_settings['conditions'][condition]['arms']:
            ban_messages.append(self.experiment_settings['conditions'][condition]['arms'][arm]["pm_text"])
            ban_subjects.append(self.experiment_settings['conditions'][condition]['arms'][arm]["pm_subject"])            

        # does this require paging??
        messages = self.r.get_mod_mail(self.subreddit)

        for message in messages:
            # if message is one of our ban messages 
                if len(message.replies) > 0:
                    for reply in message.replies:
                        # if reply id is not in  
                        if reply.body:
                               pass   


    # this method is not done / tested
    def send_ban_message(self, experiment_thing, group="control"):
        if group == "control":
            # currently, don't plan on sending messages to "control" users
            return

        metadata = json.loads(experiment_thing.metadata_json)
        treatment_arm = int(metadata['randomization']['treatment'])
        condition     = metadata['condition']
        modaction = metadata["shadow_modaction"]
        username = modaction["target_author"]

        if group == "treatment":
            pm_subject = self.experiment_settings['conditions'][condition]['arms']["arm_" + str(treatment_arm)]["pm_subject"]
            pm_text = self.experiment_settings['conditions'][condition]['arms']["arm_" + str(treatment_arm)]["pm_text"]


        if not hasattr(self, "main_sub"):
            self.main_sub = self.r.get_subreddit(self.subreddit)
        self.r.send_message(username, pm_subject, pm_text, self.main_sub)

        # add ExperimentAction
        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action_subject_type = ThingType.USER.value,
            action_subject_id = self.subreddit, ########
            action = "InterventionMessage",
            action_object_type = ThingType.USER.value,
            action_object_id = experiment_thing.id,
            metadata_json = json.dumps({
                "group": group, 
                "condition": condition,
                "arm":"arm_" + str(treatment_arm),
                "randomization": metadata['randomization'],
                "shadow_modaction": modaction, # original shadow mod action
                "main_modaction_id": main_mod_action_id,
                "message_id": None ###### TODO ?????
            }
        ))
        self.db_session.add(experiment_action)
        self.db_session.commit()

        self.log.info("{0}: Experiment {1} sent PM to username {5}, user id {6}; group {2}, condition {3}, treatment_arm {4}".format(
            self.__class__.__name__,
            self.experiment_name, 
            group,
            condition,
            treatment_arm,
            username,
            experiment_thing.id
        ))

        return experiment_action.id



    # get username from experiment_thing.id
    # only need modaction!=None if group="control"
    def apply_ban(self, experiment_thing, group="control"):
        if(self.user_acceptable(experiment_thing.id) == False):
            return None

        metadata = json.loads(experiment_thing.metadata_json)
        treatment_arm = int(metadata['randomization']['treatment'])
        condition     = metadata['condition']
        modaction = metadata["shadow_modaction"]
        username = modaction["target_author"]

        #####################
        message_text = ""
        # determine ban action parameters
        ban_reason = None
        duration = None
        if group == "treatment":
            ban_reason = self.experiment_settings['conditions'][condition]['arms']["arm_" + str(treatment_arm)]["reason_text"]
            duration = self.experiment_settings['conditions'][condition]['arms']["arm_" + str(treatment_arm)]["duration"]
        elif group == "control":
            ban_reason = modaction['description']
            try:
                duration = parse_days_from_details(modaction["details"])
            except:
                self.log.error("{0}: Experiment {1} failed to make_control_nonaction on experiment_thing id {2}. error={3}".format(
                    self.__class__.__name__,
                    self.experiment_name,
                    experiment_thing.id,
                    sys.exc_info()[0]))
                return

        # add ban
        if not hasattr(self, "main_sub"):
            self.main_sub = self.r.get_subreddit(self.subreddit)
        try:
            if not duration: #perma ban
                self.main_sub.add_ban(username, ban_reason=ban_reason) # can't see??: ban_message=message_text)
            else: # temp ban
                self.main_sub.add_ban(username, ban_reason=ban_reason, duration=duration)  # can't see??: ban_message=message_text)
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

        mod_action_query = {
            "description": ban_reason,
            "subreddit": self.subreddit,
            "target_author": username,
            "action":"banuser",
            "details":"{0} days".format(duration) if duration else "permanent"
        }
        main_mod_action_id = self.find_latest_mod_action_id_with(mod_action_query)


        # add ExperimentAction
        experiment_action = ExperimentAction(
            experiment_id = self.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, self.experiment_name),
            action_subject_type = ThingType.USER.value,
            action_subject_id = modaction["mod"], ########
            action = "Intervention",
            action_object_type = ThingType.USER.value,
            action_object_id = experiment_thing.id,
            metadata_json = json.dumps({
                "group":group, 
                "condition":condition,
                "arm":"arm_" + str(treatment_arm),
                "randomization": metadata['randomization'],
                "shadow_modaction": modaction, # original shadow mod action
                "main_modaction_id": main_mod_action_id
            }
        ))
        self.db_session.add(experiment_action)
        self.db_session.commit()

        self.log.info("{0}: Experiment {1} applied group {2}, condition {3}, treatment_arm {4} to username {5}, user id {6}".format(
            self.__class__.__name__,
            self.experiment_name, 
            group,
            condition,
            treatment_arm,
            username,
            experiment_thing.id
        ))

        return experiment_action.id

    def make_control_nonaction(self, experiment_thing, group="control"):
        return self.apply_ban(experiment_thing, group=group)

    def apply_temp_ban(self, experiment_thing, group="treatment"):
        return self.apply_ban(experiment_thing, group=group)

    def apply_perma_ban(self, experiment_thing, group="treatment"):
        return self.apply_ban(experiment_thing, group=group)

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
            json.loads(experiment_thing.metadata_json)["shadow_modaction"]["details"],
            json.loads(experiment_thing.metadata_json)["shadow_modaction"]["description"]
            ))
        return self.make_control_nonaction(experiment_thing, group="control")
        
    ## TREATMENT GROUP 1: perma ban
    def intervene_main_arm_1(self, experiment_thing):
        self.log.info(">>>Attempting intervene_main_arm_1 perma ban on user {0}, details={1}, description={2}".format(
            experiment_thing.id,
            json.loads(experiment_thing.metadata_json)["shadow_modaction"]["details"],
            json.loads(experiment_thing.metadata_json)["shadow_modaction"]["description"]
            ))
        return self.apply_perma_ban(experiment_thing, group="treatment")

    ## TREATMENT GROUP 2: temp ban
    def intervene_main_arm_2(self, experiment_thing):
        self.log.info(">>>Attempting intervene_main_arm_2 temp ban on user {0}, details={1}, description={2}".format(
            experiment_thing.id,
            json.loads(experiment_thing.metadata_json)["shadow_modaction"]["details"],
            json.loads(experiment_thing.metadata_json)["shadow_modaction"]["description"]
            ))
        return self.apply_temp_ban(experiment_thing, group="treatment")