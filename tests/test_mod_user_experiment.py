import pytest
import os, yaml
import copy
import time

## SET UP THE DATABASE ENGINE
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] = "test"

from mock import Mock, patch
import unittest.mock
import simplejson as json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
import glob, datetime, time, pytz
from app.controllers.mod_user_experiment_controller import *

from utils.common import *
from dateutil import parser
import praw, csv, random, string
from collections import Counter

### LOAD THE CLASSES TO TEST
from app.models import *
import app.cs_logger


db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)

def clear_all_tables():
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()
    db_session.query(Comment).delete()
    db_session.query(ModAction).delete()
    db_session.query(UserMetadata).delete()    
    db_session.query(Experiment).delete()
    db_session.query(ExperimentThing).delete()
    db_session.query(ExperimentAction).delete()
    db_session.query(ExperimentThingSnapshot).delete()
    db_session.query(EventHook).delete()
    db_session.commit()    

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()


def randstring(n):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

BANUSER_TEMPLATE_MOD_ACTION_JSON = None
with open("{script_dir}/fixture_data/banuser_mod_action.json".format(script_dir=TEST_DIR), "r") as f:
    BANUSER_TEMPLATE_MOD_ACTION_JSON = json.loads(f.read())

def create_banuser_mod_action(target_fullname, target_author, subreddit_name,  subreddit_id, created_utc=1500000000.0):
    new_mod_action = copy.deepcopy(BANUSER_TEMPLATE_MOD_ACTION_JSON)
    # modify the fields we care about
    new_id = "ModAction_" + randstring(len(new_mod_action["id"]) - new_mod_action["id"].index("_"))  
    new_mod_action["id"] = new_id 
    new_mod_action["target_fullname"] = "t2_{0}".format(target_fullname)
    new_mod_action["target_author"] = target_author
    new_mod_action["subreddit"] = subreddit_name
    new_mod_action["sr_id36"] = subreddit_id
    new_mod_action["created_utc"] = created_utc

    return json2obj(json.dumps(new_mod_action))


def load_mod_actions(subreddit_name, subreddit_id):
    mod_action_fixtures = []
    for filename in sorted(glob.glob("{script_dir}/fixture_data/mod_action*".format(script_dir=TEST_DIR))):
        with open(filename, "r") as f:
            mod_action_list = []
            for mod_action in json.loads(f.read()):
                mod_action['sr_id36'] = subreddit_id
                mod_action['subreddit'] = subreddit_name
                mod_action_list.append(json2obj(json.dumps(mod_action)))
                
            mod_action_fixtures.append(mod_action_list)
    return mod_action_fixtures


##########################################################

##TODO FOR TESTS

## TEST THE pre function, which archives the main subreddit
##      query_and_archive_banned_users_main
##      Fixtures and mocks:
#       - past mod log for main
#         - test cases where the accounts are older than oldest_mod_action_created_utc
#         - test creation of new user metadata records
#         - cases where we can get info about the user
#         - cases where the user is not queryable from the reddit api because it's been removed from the system
#       - more recent updates to main mod log
#       - mod actions in shadow subreddit
@patch('praw.Reddit', autospec=True)
def test_query_and_archive_banned_users_main(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    experiment_name = "mod_user_test"
    with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
            experiment_config = yaml.load(f)['test']


    controller = ModeratorExperimentController(experiment_name, db_session, r, log)

    mod_action_fixtures = load_mod_actions(controller.subreddit, controller.subreddit_id)

    r.get_subreddit.return_value = json2obj('{{"name":"{0}", "id":"{1}", "display_name":"{2}"}}'.format(experiment_config['subreddit'], experiment_config['subreddit_id'], experiment_config['subreddit']))
    r.get_mod_log.return_value = mod_action_fixtures[0] #contains one banuser record
    # test without oldest_mod_action_created_utc 

    dates = [None, datetime.datetime(2017, 5, 1, 1)]
    for d in dates:
        # for asserts later
        banuser_count = {}
        for mod_action in mod_action_fixtures[0]:
            if (not d or (d and datetime.datetime.fromtimestamp(mod_action.created_utc) >= d)) and mod_action.action == "banuser":
                uid = mod_action.target_fullname.replace("t2_", "")
                if uid not in banuser_count:
                     banuser_count[uid] = 0
                banuser_count[uid] += 1     
                
        assert db_session.query(ModAction).count() == 0
        controller.query_and_archive_banned_users_main(oldest_mod_action_created_utc=d)
        
        ## TEST THAT THE PROPER NUMBER OF ACTIONS ARE ADDED TO THE DATABASE
        # all ModActions are always stored 
        assert db_session.query(ModAction).count() == len(mod_action_fixtures[0])
        
        # which UserMetadata records are stored depends on oldest_mod_action_created_utc
        usermetadata = db_session.query(UserMetadata).all()
        assert len(usermetadata) == len(banuser_count)
        
        for user in usermetadata:
            assert int(user.field_value) == banuser_count[user.user_id]

        clear_all_tables()

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_get_eligible_users_and_archive_mod_actions(mock_subreddit, mock_reddit):
    # eligible users = first-time-banned users
    #   don't have existing modactions in main subreddit
    #   only have 1 modaction in shadow subreddit
    #   don't have existing ExperimentThing
    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "mod_user_test": ModUserExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']


        assert(len(db_session.query(Experiment).all()) == 0)
        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)
        assert(len(db_session.query(Experiment).all()) == 1)


        # remove banuser actions for now, reconstruct later
        user_to_ban_action = {} # {username: [modaction, modaction]}
        main_mod_action_fixtures = load_mod_actions(controller_instance.subreddit, controller_instance.subreddit_id)
        shadow_mod_action_fixtures = load_mod_actions(controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)


        main_mod_actions = main_mod_action_fixtures[0]
        shadow_mod_actions = shadow_mod_action_fixtures[1]        
        this_shadow_mod_actions = shadow_mod_action_fixtures[2]                

        #modify: first remove banuser actions
        for mod_actions in [main_mod_actions, shadow_mod_actions, this_shadow_mod_actions]:
            for i, mod_action in enumerate(mod_actions):
                if mod_action.action == "banuser":
                    mod_actions.pop(i)

        # not eligible: user main banned, shadow banned, this time shadow banned once
        user_a = "ymysb"
        main_mod_actions.append(create_banuser_mod_action(user_a, user_a, controller_instance.subreddit, controller_instance.subreddit_id))
        shadow_mod_actions.append(create_banuser_mod_action(user_a, user_a, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))
        this_shadow_mod_actions.append(create_banuser_mod_action(user_a, user_a, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))

        #    not eligible: user main banned, not shadow banned, this time shadow banned once
        user_b = "ymnsb"
        main_mod_actions.append(create_banuser_mod_action(user_b, user_b, controller_instance.subreddit, controller_instance.subreddit_id))
        this_shadow_mod_actions.append(create_banuser_mod_action(user_b, user_b, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))

        # not eligible: not user main banned, shadow banned, this time shadow banned once           
        user_c = "nmysb"
        shadow_mod_actions.append(create_banuser_mod_action(user_c, user_c, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))
        this_shadow_mod_actions.append(create_banuser_mod_action(user_c, user_c, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))

        # not eligible: not user main banned, not shadow banned, this time shadow banned more than once                   
        user_d = "nmnsbb"
        for i in range(2):
            this_shadow_mod_actions.append(create_banuser_mod_action(user_d, user_d, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))

        # not eligible: not user main banned, not shadow banned, this time shadow banned once, have ExperimentThing   
        user_e = "nmnsbe"
        this_shadow_mod_actions.append(create_banuser_mod_action(user_e, user_e, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))
        experiment_thing = ExperimentThing(
                id             = user_e,
                object_type    = ThingType.USER.value,
                experiment_id  = controller_instance.experiment.id,
                object_created = None,
                metadata_json  = None
            )
        db_session.add(experiment_thing)
        db_session.commit()

        # eligible: not user main banned, not shadow banned, this time shadow banned once, no ExperimentThing   
        user_f = "nmnsb"
        this_shadow_mod_actions.append(create_banuser_mod_action(user_f, user_f, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))

        user_g = "too_old"
        this_shadow_mod_actions.append(create_banuser_mod_action(user_g, user_g, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id, created_utc=1400000000.0))

        user_h = "too_new"        
        this_shadow_mod_actions.append(create_banuser_mod_action(user_h, user_h, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id, created_utc=1700000000.0))

        ############

        assert db_session.query(UserMetadata).count() == 0

        # 1) pre test: load ModActions, UserMetadata from main subreddit
        r.get_mod_log.return_value = main_mod_actions
        r.get_subreddit.return_value = json2obj('{{"name":"{0}", "id":"{1}", "display_name":"{2}"}}'.format(
            experiment_settings['subreddit'], 
            experiment_settings['subreddit_id'], 
            experiment_settings['subreddit']))
        patch('praw.')
        controller_instance.query_and_archive_banned_users_main() ########## <<<<

        main_user_metadata = db_session.query(UserMetadata).filter(UserMetadata.subreddit_id==controller_instance.subreddit_id).all()
        assert len(main_user_metadata) == 2
        for umd in main_user_metadata:
            if umd.user_id == user_a:
                assert umd.field_value == str(1)
            elif umd.user_id == user_b:
                assert umd.field_value == str(1)            
            else:
                assert False



        # 2) 1st shadow load: load UserMetadata from shadow subreddit (we are not testing ModActions so don't need to load those.)
        r.get_subreddit.return_value = json2obj('{{"name":"{0}", "id":"{1}", "display_name":"{2}"}}'.format(
            experiment_settings['shadow_subreddit'], 
            experiment_settings['shadow_subreddit_id'], 
            experiment_settings['shadow_subreddit']))
        patch('praw.')
        mod_instance = ModeratorController(controller_instance.shadow_subreddit, db_session, r, log)
        mod_instance.mod_actions = shadow_mod_actions
        assert len(mod_instance.mod_actions) == 502
        
        banned_user_id_to_modaction = controller_instance.get_eligible_users_and_archive_mod_actions(mod_instance) ############ <<<<
        assert(len(banned_user_id_to_modaction.keys())) == 1
        assert user_c in banned_user_id_to_modaction

        main_user_metadata = db_session.query(UserMetadata).filter(UserMetadata.subreddit_id==controller_instance.subreddit_id).all()
        assert len(main_user_metadata) == 2 #still 

        shadow_user_metadata = db_session.query(UserMetadata).filter(UserMetadata.subreddit_id==controller_instance.shadow_subreddit_id).all()
        assert len(shadow_user_metadata) == 2
        for umd in shadow_user_metadata:
            if umd.user_id == user_a:
                assert umd.field_value == str(1)
            elif umd.user_id == user_c:
                assert umd.field_value == str(1)
            else:
                assert False



        # 3) 2nd shadow load: instance.mod_actions should have new mod actions on shadow subreddit
        mod_instance.mod_actions = this_shadow_mod_actions
        assert len(mod_instance.mod_actions) == 509 # 500 existing, and mod actions for user_a through user_h (2 for user_d)

        banned_user_id_to_modaction = controller_instance.get_eligible_users_and_archive_mod_actions(mod_instance) ############ <<<<
        assert(len(banned_user_id_to_modaction.keys())) == 1 # user_f
        assert user_f in banned_user_id_to_modaction

        main_user_metadata = db_session.query(UserMetadata).filter(UserMetadata.subreddit_id==controller_instance.subreddit_id).all()
        assert len(main_user_metadata) == 2 #still           

        shadow_user_metadata = db_session.query(UserMetadata).filter(UserMetadata.subreddit_id==controller_instance.shadow_subreddit_id).all()
        assert len(shadow_user_metadata) == 6
        for umd in shadow_user_metadata:
            if umd.user_id == user_a:
                assert umd.field_value == str(2)
            elif umd.user_id == user_b:
                assert umd.field_value == str(1)
            elif umd.user_id == user_c:
                assert umd.field_value == str(2)
            elif umd.user_id == user_d:
                assert umd.field_value == str(2)
            elif umd.user_id == user_e:                                
                assert umd.field_value == str(1)
            elif umd.user_id == user_f:                                
                assert umd.field_value == str(1)
            else:
                assert False


def test_parse_days_from_details():
    assert parse_days_from_details("permanent") == None
    assert parse_days_from_details("1 days") == 1

    try:
        parse_days_from_details("weird string") == 1
    except:
        assert True # should have raised Exception
    else:
        assert False


@patch('praw.Reddit', autospec=True)
def test_user_acceptable(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    experiment_name = "mod_user_test"
    controller_instance = ModUserExperimentController(experiment_name, db_session, r, log)

    user_id = "foo"
    # if no ExperimentAction, return True
    assert controller_instance.user_acceptable(user_id) == True

    ea = ExperimentAction(
        experiment_id=controller_instance.experiment.id,
        action_object_type=ThingType.USER.value,
        action_object_id=user_id,
        action="Intervention"
        )
    db_session.add(ea)
    db_session.commit()

    # if ExperimentAction, return False
    assert controller_instance.user_acceptable(user_id) == False

# this test is very similar to test_sticky_comment_experiment_controller.test_assign_randomized_conditions
@patch('praw.Reddit', autospec=True)
def test_assign_randomized_conditions(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    experiment_name = "mod_user_test"
    controller_instance = ModUserExperimentController(experiment_name, db_session, r, log)

    banned_user_to_modaction = {} ##############
    controller_instance.assign_randomized_conditions(banned_user_to_modaction)

    ############## see sticky comment test

    ## TEST THE BASE CASE OF RANDOMIZATION
    banned_user_to_modaction = {} ################### ????
    for i in range(100):
        mod_action = create_banuser_mod_action(randstring(10), randstring(10), controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)
        assert mod_action.__class__.__name__ == "X"
        user_record = User(
            id=mod_action.target_fullname.replace("t2_", ""), 
            name=mod_action.target_author)
        db_session.add(user_record)
        db_session.commit()
        banned_user_to_modaction[user_record] = mod_action
    assert len(banned_user_to_modaction) == 100 ####


    experiment_action_count = db_session.query(ExperimentAction).count()
    assert experiment_action_count == 0
    experiment_settings = json.loads(controller_instance.experiment.settings_json)
    for condition_name in experiment_settings['conditions']:
        assert experiment_settings['conditions'][condition_name]['next_randomization'] == 0            
    assert db_session.query(ExperimentThing).count()    == 0

    controller_instance.assign_randomized_conditions(banned_user_to_modaction)
    assert db_session.query(ExperimentThing).count() == 100 
    assert len(db_session.query(Experiment).all()) == 1
    experiment = db_session.query(Experiment).first()
    experiment_settings = json.loads(experiment.settings_json)


    assert experiment_settings['conditions']['main']['next_randomization'] == 100
        
    for experiment_thing in db_session.query(ExperimentThing).all():
        assert experiment_thing.id != None
        assert experiment_thing.object_type == ThingType.USER.value
        assert experiment_thing.experiment_id == controller_instance.experiment.id
        assert "randomization" in json.loads(experiment_thing.metadata_json).keys()
        assert "condition" in json.loads(experiment_thing.metadata_json).keys()
        assert "shadow_modaction" in json.loads(experiment_thing.metadata_json).keys() #####

        
    ## TEST THE CASE WHERE THE EXPERIMENT HAS CONCLUDED
    ### first step: set the condition counts to have just one remaining condition left 
    for condition_name in experiment_settings['conditions']:
        experiment_settings['conditions'][condition_name]['next_randomization'] = len(experiment_settings['conditions'][condition_name]['randomizations']) - 1
        controller_instance.experiment_settings['conditions'][condition_name]['next_randomization'] = experiment_settings['conditions'][condition_name]['next_randomization']
        experiment_settings_json = json.dumps(experiment_settings)
        db_session.commit()


    new_banned_user_to_modaction = {} ################### ????
    for i in range(4):
        mod_action = create_banuser_mod_action(randstring(10), randstring(10), controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)
        user_record = User(
            id=mod_action.target_fullname.replace("t2_", ""), 
            name=mod_action.target_author)
        db_session.add(user_record)
        db_session.commit()
        new_banned_user_to_modaction[user_record] = mod_action
    assert len(new_banned_user_to_modaction) == 4 ####

    # Only 1 randomization left for each condition, while there are >1 new_posts
    experiment_things = controller_instance.assign_randomized_conditions(new_banned_user_to_modaction)
    ## assert that only 1 item from each condition went through
    assert len(experiment_things) == len(experiment_settings['conditions'])
    for thing in experiment_things:
        assert thing.id in [x.id for x in new_banned_user_to_modaction]
    
    ## CHECK THE EMPTY CASE
    ## make sure that no actions are taken if the list is empty
    experiment_action_count = db_session.query(ExperimentAction).count()
    controller_instance.assign_randomized_conditions([])
    assert db_session.query(ExperimentAction).count() == experiment_action_count


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
@patch.object(ModUserExperimentController, "find_latest_mod_action_id_with")
def test_apply_ban(mock_find_latest_mod_action_id_with, mock_subreddit, mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    experiment_name = "mod_user_test"
    controller_instance = ModUserExperimentController(experiment_name, db_session, r, log)
    controller_instance.find_latest_mod_action_id_with.return_value = "ModAction_123456"

    mock_subreddit.add_ban.return_value = {'errors': []}
    mock_subreddit.display_name = controller_instance.subreddit
    mock_subreddit.name = controller_instance.subreddit
    mock_subreddit.id = controller_instance.subreddit_id    
    r.get_subreddit.return_value = mock_subreddit

    patch('praw.')

    # 1) control: test group="control", duration = None (always). should parse details correctly (make_control_nonaction)

    mod_action = create_banuser_mod_action(randstring(10), randstring(10), controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)
    experiment_thing = ExperimentThing(
        id             = mod_action.target_fullname,
        object_type    = ThingType.USER.value,
        experiment_id  = controller_instance.experiment.id,
        object_created = None, # don't need to mock this for tests
        metadata_json = json.dumps({
            "randomization": {
                "treatment":0, # control
                "block.id":"main.block001", 
                "block.size":10
                }, 
            "condition":"main",
            "shadow_modaction": mod_action
            })
    )

    ea_id = controller_instance.apply_ban(experiment_thing, group="control") ###### <<<<
    assert ea_id is not None
    ea = db_session.query(ExperimentAction).filter(ExperimentAction.id==ea_id).all()
    assert len(ea) == 1
    ea = ea[0]
    metadata = json.loads(ea.metadata_json)
    assert metadata["arm"] == "arm_0"
    assert "shadow_modaction" in metadata and metadata["shadow_modaction"] is not None
    assert "randomization" in metadata and metadata["randomization"] is not None
    assert metadata["randomization"]["treatment"] == 0


    # 2) perma ban: test group="treatment", duration = None (test_apply_perma_ban)
    # 3) treatment ban: test group="treatment", duration = 7 (test_apply_temp_ban)
    # duration, reason_text specified in experiment yml
    for treatment_arm in [1, 2]:
        mod_action = create_banuser_mod_action(randstring(10), randstring(10), controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)
        experiment_thing = ExperimentThing(
            id             = mod_action.target_fullname.replace("t2_", ""),
            object_type    = ThingType.USER.value,
            experiment_id  = controller_instance.experiment.id,
            object_created = None, # don't need to mock this for tests
            metadata_json = json.dumps({
                "randomization": {
                    "treatment":treatment_arm, # control
                    "block.id":"main.block001", 
                    "block.size":10
                    }, 
                "condition":"main",
                "shadow_modaction": mod_action
                })
        )

        ea_id = controller_instance.apply_ban(experiment_thing, group="treatment")  ###### <<<<
        assert ea_id is not None
        ea = db_session.query(ExperimentAction).filter(ExperimentAction.id==ea_id).all()
        assert len(ea) == 1
        ea = ea[0]
        metadata = json.loads(ea.metadata_json)
        assert metadata["arm"] == "arm_" + str(treatment_arm)        
        assert "shadow_modaction" in metadata and metadata["shadow_modaction"] is not None
        assert "randomization" in metadata and metadata["randomization"] is not None
        assert metadata["randomization"]["treatment"] == treatment_arm




@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
@patch.object(ModUserExperimentController, "query_and_archive_new_banned_users_main")
@patch.object(ModUserExperimentController, "apply_ban")
@patch.object(ModUserExperimentController, "conclude_intervention")
def test_update_experiment(mock_conclude_intervention, mock_apply_ban, mock_query_and_archive_new_banned_users_main, 
                            mock_subreddit, mock_reddit):
    r = mock_reddit.return_value    

    r.get_redditor.side_effect = Mock(side_effect=praw.errors.NotFound) # just make all users not found
 
    experiment_name = "mod_user_test"
    controller_instance = ModUserExperimentController(experiment_name, db_session, r, log)

    # for ModeratorController
    mock_subreddit.display_name = controller_instance.shadow_subreddit
    mock_subreddit.name = controller_instance.shadow_subreddit
    mock_subreddit.id = controller_instance.shadow_subreddit_id    
    r.get_subreddit.return_value = mock_subreddit
    patch('praw.')
    mod_instance = ModeratorController(controller_instance.shadow_subreddit, db_session, r, log)

    shadow_mod_action_fixtures = load_mod_actions(controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)
    mod_instance.mod_actions = shadow_mod_action_fixtures[0]
    for i in range(10):
        # add 10 more banactions
        mod_instance.mod_actions.append(create_banuser_mod_action(
            randstring(10), randstring(10), 
            controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id))

    assert mock_query_and_archive_new_banned_users_main.called == False
    assert mock_apply_ban.called == False
    assert mock_conclude_intervention.called == False
    controller_instance.query_and_archive_new_banned_users_main.return_value = None # don't do anything
    controller_instance.apply_ban.return_value = 129874 # "mocked" experiment_action.id
    controller_instance.conclude_intervention.return_value = None

    experiment_return = controller_instance.update_experiment(mod_instance)

    ## ASSERT THAT THE METHODS FOR TAKING ACTIONS ARE CALLED
    assert mock_query_and_archive_new_banned_users_main.called == True
    assert mock_apply_ban.called == True
    assert mock_conclude_intervention.called == True

    assert db_session.query(Experiment).count() == 1
    assert db_session.query(ModAction).count() == 0 # update experiment doesn't store any shadow mod actions. it will store main mod actions, but we didn't load any
    assert db_session.query(UserMetadata).count() == 12    
    assert db_session.query(ExperimentThing).count() == 12
    assert db_session.query(ExperimentAction).count() == 0 # this test does not call intervene_ methods
    assert len(experiment_return) == 12



@patch('praw.Reddit', autospec=True)
def test_archive_user_records(mock_reddit):
    # for each username in banned_user_id_to_modaction, store User record
    # returns dictionary of {User record: praw.objects.ModAction}
    r = mock_reddit.return_value

    experiment_name = "mod_user_test"
    controller_instance = ModUserExperimentController(experiment_name, db_session, r, log)



    user_a = "user_a"   # already existed
    user_b = "user_b"   # could not find when queried
    user_c = "user_c"   # found when queried

    # db commit
    user_a_record = User(id=user_a, name=user_a, last_seen=datetime.datetime.utcnow())
    user_a_last_seen = user_a_record.last_seen
    db_session.add(user_a_record)
    db_session.commit()

    count = 1
    assert db_session.query(User).count() == count

    test_users = [user_a, user_b, user_c]
    for user_str in test_users: # test one at a time to make mocking get_redditor easier
        if user_str == user_a:
            # don't need to mock get_redditor
            time.sleep(1) # just so user_a_last_seen can get old enough
        elif user_str == user_b:
            count += 1    
            r.get_redditor.side_effect = Mock(side_effect=praw.errors.NotFound) 
        elif user_str == user_c:
            count += 1    
            r.get_redditor.side_effect = None
            with open("{script_dir}/fixture_data/user.json".format(script_dir=TEST_DIR)) as f:
                data = json.loads(f.read())
                data["id"] = user_str
                data["name"] = user_str
                r.get_redditor.return_value = json2obj(json.dumps(data))
        patch('praw.')

        banned_user_id_to_modaction = {
            user_str: create_banuser_mod_action(user_str, user_str, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)}
        banned_user_to_modaction = controller_instance.archive_user_records(banned_user_id_to_modaction)

        assert db_session.query(User).count() == count # testing one at a time

        user_record = list(banned_user_to_modaction.keys())[0]
        assert type(user_record) == User # models.User
        
        if user_str == user_a:
            assert user_record.name == banned_user_id_to_modaction[user_str].target_author
            assert user_record.id == banned_user_id_to_modaction[user_str].target_fullname.replace("t2_", "")
            assert user_record.last_seen > user_a_last_seen
            assert user_record.user_data is None
        elif user_str == user_b:
            assert user_record.name == banned_user_id_to_modaction[user_str].target_author
            assert user_record.id == banned_user_id_to_modaction[user_str].target_fullname.replace("t2_", "")
            assert user_record.user_data is None
        elif user_str == user_c:
            assert user_record.name == banned_user_id_to_modaction[user_str].target_author            
            assert user_record.id == banned_user_id_to_modaction[user_str].target_fullname.replace("t2_", "")
            assert user_record.created is not None
            assert user_record.user_data is not None



@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_conclude_intervention(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value
    mock_subreddit.remove_ban.return_value = {'errors': []}

    patch('praw.')

    experiment_name = "mod_user_test"
    controller_instance = ModUserExperimentController(experiment_name, db_session, r, log)

    mock_subreddit.add_ban.return_value = {'errors': []}
    mock_subreddit.display_name = controller_instance.subreddit
    mock_subreddit.name = controller_instance.subreddit
    mock_subreddit.id = controller_instance.subreddit_id    
    r.get_subreddit.return_value = mock_subreddit

    patch('praw.')

    # user_a has ExperimentThing, not old "Intervention" ExperimentAction > do nothing
    user_a = "user_a"

    # user_b has ExperimentThing, old "Intervention" ExperimentAction, "ConcludeIntervention" ExperimentAction : do nothing
    user_b = "user_b"

    # user_c has ExperimentThing, old "Intervention" ExperimentAction > unban
    user_c = "user_c"

    too_old_time = datetime.datetime.utcnow() - datetime.timedelta(days=controller_instance.experiment_settings['max_ban_duration']+2)
    not_old_time = datetime.datetime.utcnow() - datetime.timedelta(days=controller_instance.experiment_settings['max_ban_duration']-2)    
    
    randomization = {
            "treatment":"control",
            "block.id":"main.block001", 
            "block.size":10
        }
    for user in [user_a, user_b, user_c]:
        shadow_mod_action = create_banuser_mod_action(user, user, controller_instance.shadow_subreddit, controller_instance.shadow_subreddit_id)
        main_mod_action = create_banuser_mod_action(user, user, controller_instance.subreddit, controller_instance.subreddit_id)        
        experiment_thing = ExperimentThing(
            id             = shadow_mod_action.target_fullname.replace("t2_", ""),
            object_type    = ThingType.USER.value,
            experiment_id  = controller_instance.experiment.id,
            object_created = None, # don't need to mock this for tests
            metadata_json = json.dumps({
                "randomization":randomization, 
                "condition":"main",
                "shadow_modaction": shadow_mod_action
                })
        )
        db_session.add(experiment_thing)

        if user is user_a:
            created_at = not_old_time
        elif user is user_b or user is user_c:
            created_at = too_old_time

        intervene_experiment_action = ExperimentAction(
            created_at = created_at,
            experiment_id = controller_instance.experiment.id,
            praw_key_id = PrawKey.get_praw_id(ENV, controller_instance.experiment_name),
            action_subject_type = ThingType.USER.value,
            action_subject_id = shadow_mod_action.mod, ########
            action = "Intervention",
            action_object_type = ThingType.USER.value,
            action_object_id = user,
            metadata_json = json.dumps({
                "group":"control", 
                "condition":"main",
                "arm":"arm_0",
                "randomization": randomization,
                "shadow_modaction": shadow_mod_action, # original shadow mod action
                "main_modaction_id": main_mod_action.id
            }
        ))
        db_session.add(intervene_experiment_action)        

        if user is user_b:
            conclude_experiment_action = ExperimentAction(
                experiment_id = controller_instance.experiment.id,
                praw_key_id = PrawKey.get_praw_id(ENV, controller_instance.experiment_name),
                action_subject_type = ThingType.USER.value,
                action_subject_id = shadow_mod_action.mod, ########
                action = "ConcludeIntervention",
                action_object_type = ThingType.USER.value,
                action_object_id = user,
                metadata_json = json.dumps({
                    "intervention_id": intervene_experiment_action.id,
                    "shadow_modaction_id": shadow_mod_action.id, # original shadow mod action
                    "main_modaction_id": main_mod_action.id
                }
            ))
            db_session.add(conclude_experiment_action)        

    db_session.commit()

    ###########
    assert db_session.query(ExperimentAction).filter(ExperimentAction.action == "ConcludeIntervention").count() == 1    
    new_ea_ids = controller_instance.conclude_intervention()
    assert len(new_ea_ids) == 1
    assert db_session.query(ExperimentAction).filter(ExperimentAction.action == "ConcludeIntervention").count() == 2


"""
"""