import pytest
import os, yaml

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
from app.controllers.sticky_comment_experiment_controller import *
from app.controllers.front_page_controller import FrontPageController

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


### TODO: REFACTOR THIS INTO A SUPERCLASS FOR EXPERIMENTS
@patch('praw.Reddit', autospec=True)
def test_initialize_experiment(mock_reddit):
    r = mock_reddit.return_value
    patch('praw.')

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR,"config", "experiments", experiment_name + ".yml"), "r") as f:
            experiment_config = yaml.load(f)['test']

        assert(len(db_session.query(Experiment).all()) == 0)

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        assert(len(db_session.query(Experiment).all()) == 1)
        experiment = db_session.query(Experiment).first()
        assert(experiment.name       == experiment_name)
        assert(experiment.controller == experiment_config['controller'])
        assert(pytz.timezone("UTC").localize(experiment.start_time) == parser.parse(experiment_config['start_time']))
        assert(pytz.timezone("UTC").localize(experiment.end_time)   == parser.parse(experiment_config['end_time']))
        
        settings = json.loads(experiment.settings_json)
        for k in ['username', 'subreddit', 'subreddit_id','max_eligibility_age', 
                  'min_eligibility_age', 'start_time', 'end_time', 'controller']:
            assert(settings[k] == experiment_config[k])

        ### NOW TEST THAT AMA OBJECTS ARE ADDED
        for condition_name in experiment_config['conditions']:
            with open(os.path.join(BASE_DIR,"config", "experiments", experiment_config['conditions'][condition_name]['randomizations']), "r") as f:
                conditions = []
                for row in csv.DictReader(f):
                    conditions.append(row)

            with open(os.path.join(BASE_DIR,"config", "experiments", experiment_config['conditions'][condition_name]['randomizations']), "r") as f:
                nonconditions = []
                for row in csv.DictReader(f):
                    nonconditions.append(row)

            assert len(settings['conditions'][condition_name]['randomizations']) == len(conditions)
            assert settings['conditions'][condition_name]['next_randomization']     == 0

        clear_all_tables()

# calls set_eligible_objects, which for FrontPageStickyCommentExperimentController has a different signature
# (is a callback function), so we don't test FrontPageStickyCommentExperimentController here
@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_get_eligible_objects(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_ama_2020_test": AMA2020StickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        sub_data = []
        with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
            fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
            min_age = experiment_settings["min_eligibility_age"]
            for post in fixture:
                json_dump = json.dumps(post)
                postobj = json2obj(json_dump, now=True, offset=-1*min_age)
                sub_data.append(postobj)

        mock_subreddit.get_new.return_value = sub_data
        mock_subreddit.display_name = experiment_settings['subreddit']
        mock_subreddit.name = experiment_settings['subreddit']
        mock_subreddit.id = experiment_settings['subreddit_id']
        r.get_subreddit.return_value = mock_subreddit
        patch('praw.')

        assert(len(db_session.query(Experiment).all()) == 0)
        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)
        assert(len(db_session.query(Experiment).all()) == 1)

        # "mock" FrontPageController.posts
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            instance = FrontPageController(db_session, r, log)
            instance.posts = sub_data

        
        ### TEST THE METHOD FOR FETCHING ELIGIBLE OBJECTS
        ### FIRST TIME AROUND
        assert len(db_session.query(Post).all()) == 0

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects(instance)
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()
        elif controller_instance.__class__ is AMA2020StickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()
            for obj in objs[::10]:
                obj.json_dict["link_flair_css_class"] = ""
        eligible_objects = controller_instance.get_eligible_objects(objs)
        if controller_instance.__class__ is AMA2020StickyCommentExperimentController:
            expected_eligible_count = 90
        else:
            expected_eligible_count = 100
        assert len(eligible_objects) == expected_eligible_count

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            eligible_submissions = {sub.id: sub for sub in eligible_objects}
            controller_instance.archive_eligible_submissions(eligible_submissions)
        assert len(db_session.query(Post).all()) == 100

        ### TEST THE METHOD FOR FETCHING ELIGIBLE OBJECTS
        ### SECOND TIME AROUND, WITH SOME ExperimentThing objects stored
        limit = 50
        #for post in db_session.query(Post).all():
        for post in eligible_objects:
            limit -= 1
            experiment_thing = ExperimentThing(
                id = post.id, 
                object_type=int(ThingType.SUBMISSION.value),
                experiment_id = controller_instance.experiment.id)
            db_session.add(experiment_thing)
            if(limit <= 0):
                break
        db_session.commit()

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects(instance)
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()
        elif controller_instance.__class__ is AMA2020StickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()
        eligible_objects = controller_instance.get_eligible_objects(objs)
        if controller_instance.__class__ is AMA2020StickyCommentExperimentController:
            expected_eligible_count = 40
        else:
            expected_eligible_count = 50
        assert len(eligible_objects) == expected_eligible_count
        clear_all_tables()    

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_assign_randomized_conditions(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        sub_data = []
        with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
            fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
            min_age = experiment_settings["min_eligibility_age"]
            for post in fixture:
                json_dump = json.dumps(post)
                postobj = json2obj(json_dump, now=True, offset=-1*min_age)
                sub_data.append(postobj)
        mock_subreddit.get_new.return_value = sub_data
        mock_subreddit.display_name = experiment_settings['subreddit']
        mock_subreddit.name = experiment_settings['subreddit']
        mock_subreddit.id = experiment_settings['subreddit_id']
        r.get_subreddit.return_value = mock_subreddit
        patch('praw.')

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        # "mock" FrontPageController.posts
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            instance = FrontPageController(db_session, r, log)
            instance.posts = sub_data


        ## TEST THE BASE CASE OF RANDOMIZATION
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects(instance)
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()            
        eligible_objects = controller_instance.get_eligible_objects(objs)
        assert len(eligible_objects) == 100 ####


        experiment_action_count = db_session.query(ExperimentAction).count()
        experiment_settings = json.loads(controller_instance.experiment.settings_json)
        for condition_name in experiment_settings['conditions']:
            assert experiment_settings['conditions'][condition_name]['next_randomization']    == 0            
        assert db_session.query(ExperimentThing).count()    == 0

        controller_instance.assign_randomized_conditions(eligible_objects)
        assert db_session.query(ExperimentThing).count() == 100 
        assert len(db_session.query(Experiment).all()) == 1
        experiment = db_session.query(Experiment).first()
        experiment_settings = json.loads(experiment.settings_json)


        if controller_instance.__class__ is AMAStickyCommentExperimentController:
            assert experiment_settings['conditions']['ama']['next_randomization'] == 2
            assert experiment_settings['conditions']['nonama']['next_randomization'] == 98
            assert sum([x['next_randomization'] for x in list(experiment_settings['conditions'].values())]) == 100
        elif controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            assert experiment_settings['conditions']['frontpage_post']['next_randomization'] == 100
            
        for experiment_thing in db_session.query(ExperimentThing).all():
            assert experiment_thing.id != None
            assert experiment_thing.object_type == ThingType.SUBMISSION.value
            assert experiment_thing.experiment_id == controller_instance.experiment.id
            assert "randomization" in json.loads(experiment_thing.metadata_json).keys()
            assert "condition" in json.loads(experiment_thing.metadata_json).keys()
            
        ## TEST THE CASE WHERE THE AMA EXPERIMENT HAS CONCLUDED
        ### first step: set the condition counts to have just one remaining condition left 
        for condition_name in experiment_settings['conditions']:
            experiment_settings['conditions'][condition_name]['next_randomization'] = len(experiment_settings['conditions'][condition_name]['randomizations']) - 1
            controller_instance.experiment_settings['conditions'][condition_name]['next_randomization'] = experiment_settings['conditions'][condition_name]['next_randomization']
            experiment_settings_json = json.dumps(experiment_settings)
            db_session.commit()


        posts = []  
        if controller_instance.__class__ is AMAStickyCommentExperimentController:        
            posts += [x for x in sub_data if "ama" in x.link_flair_css_class][0:2]
            posts += [x for x in sub_data if "ama" not in x.link_flair_css_class][0:2]
        elif controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            posts += sub_data[0:4]

        ## generate new fake ids for these fixture posts, 
        ## which would otherwise be duplicates
        new_posts = []
        for post in posts:
            post = post.json_dict
            post['id'] = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(7))
            new_posts.append(json2obj(json.dumps(post)))

        # Only 1 randomization left for each condition, while there are >1 new_posts
        assert len(new_posts) == 4
        experiment_things = controller_instance.assign_randomized_conditions(new_posts)
        ## assert that only 1 item from each condition went through
        assert len(experiment_things) == len(experiment_settings['conditions'])
        for thing in experiment_things:
            assert thing.id in [x.id for x in new_posts]
        
        ## CHECK THE EMPTY CASE
        ## make sure that no actions are taken if the list is empty
        experiment_action_count = db_session.query(ExperimentAction).count()
        controller_instance.assign_randomized_conditions([])
        assert db_session.query(ExperimentAction).count() == experiment_action_count

        clear_all_tables()


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
@patch.object(FrontPageStickyCommentExperimentController, "intervene_frontpage_post_arm_0")
@patch.object(FrontPageStickyCommentExperimentController, "intervene_frontpage_post_arm_1")
@patch.object(AMAStickyCommentExperimentController, "intervene_nonama_arm_0")
@patch.object(AMAStickyCommentExperimentController, "intervene_nonama_arm_1")
@patch.object(AMAStickyCommentExperimentController, "intervene_ama_arm_0")
@patch.object(AMAStickyCommentExperimentController, "intervene_ama_arm_1")
def test_update_experiment(intervene_ama_arm_1, intervene_ama_arm_0, 
                            intervene_nonama_arm_1, intervene_nonama_arm_0, 
                            intervene_frontpage_post_arm_1, intervene_frontpage_post_arm_0,
                            mock_subreddit, mock_reddit):
    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:

        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        sub_data = []
        with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
            fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
            min_age = experiment_settings["min_eligibility_age"]
            for post in fixture:
                json_dump = json.dumps(post)
                postobj = json2obj(json_dump, now=True, offset=-1*min_age)
                sub_data.append(postobj)
        mock_subreddit.get_new.return_value = sub_data
        mock_subreddit.display_name = experiment_settings['subreddit']
        mock_subreddit.name = experiment_settings['subreddit']
        mock_subreddit.id = experiment_settings['subreddit_id']
        r.get_subreddit.return_value = mock_subreddit
        patch('praw.')

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        # "mock" FrontPageController.posts
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            instance = FrontPageController(db_session, r, log)
            instance.posts = sub_data



        ## GET RANDOMIZATIONS
        #objs = scec.set_eligible_objects()
        #eligible_objects = scec.get_eligible_objects(objs)
        #experiment_things = scec.assign_randomized_conditions(eligible_objects)
        assert db_session.query(ExperimentThing).count() == 0

        ## MOCK RETURN VALUES FROM ACTION METHODS
        ## This will never be saved. Just creating an object
        comment_thing = ExperimentThing(
            experiment_id = controller_instance.experiment.id,
            object_created = None,
            object_type = ThingType.COMMENT.value,
            id = "12345",
            metadata_json = json.dumps({"group":None,"submission_id":None})
        )

        # for AMAStickyCommentExperimentController
        intervene_nonama_arm_0.return_value = comment_thing
        intervene_nonama_arm_1.return_value = comment_thing
        intervene_ama_arm_0.return_value = comment_thing
        intervene_ama_arm_1.return_value = comment_thing 
        
        # for FrontPageStickyCommentExperimentController
        intervene_frontpage_post_arm_0.return_value = comment_thing 
        intervene_frontpage_post_arm_1.return_value = comment_thing

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            experiment_return = controller_instance.callback_update_experiment(instance)
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            experiment_return = controller_instance.update_experiment()


        ## ASSERT THAT THE METHODS FOR TAKING ACTIONS ARE CALLED
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            assert intervene_frontpage_post_arm_0.called == True
            assert intervene_frontpage_post_arm_1.called == True
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            assert intervene_nonama_arm_0.called == True
            assert intervene_nonama_arm_1.called == True
            assert intervene_ama_arm_0.called == True
            assert intervene_ama_arm_1.called == True
        assert len(experiment_return) == 100
        clear_all_tables()


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
@patch('praw.objects.Comment', autospec=True)
def test_submission_acceptable(mock_comment, mock_submission, mock_reddit):
    r = mock_reddit.return_value


    with open("{script_dir}/fixture_data/submission_0.json".format(script_dir=TEST_DIR)) as f:
        submission_json = json.loads(f.read())
        ## setting the submission time to be recent enough
        submission = json2obj(json.dumps(submission_json), now=True)
        mock_submission.id = submission.id
        mock_submission.json_dict = submission.json_dict

    with open("{script_dir}/fixture_data/submission_0_comments.json".format(script_dir=TEST_DIR)) as f:
        comments = json2obj(f.read())
        len_comments = len(comments)

    with open("{script_dir}/fixture_data/submission_0_treatment.json".format(script_dir=TEST_DIR)) as f:
        treatment_json = f.read()
        treatment = json2obj(treatment_json)
        treatment_dict = json.loads(treatment_json)
        treatment_dict['stickied']=True
        stickied_treatment = json2obj(json.dumps(treatment_dict))
        mock_comment.id = treatment.id
        mock_comment.created_utc = treatment.created_utc
        mock_submission.add_comment.return_value = mock_comment

    with open("{script_dir}/fixture_data/submission_0_treatment_distinguish.json".format(script_dir=TEST_DIR)) as f:
        distinguish = json.loads(f.read())
        mock_comment.distinguish.return_value = distinguish
    
    patch('praw.')


    experiment_name_to_controller = {
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController,
        "sticky_comment_0": AMAStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        # reload mock_submission.comments 
        mock_submission.comments = list(comments)

        ## First check acceptability on a submission with an old timestamp
        ## which should return None and take no action
        assert db_session.query(ExperimentAction).count() == 0
        mock_submission.created_utc = int(datetime.datetime.now().timestamp()) - 1000
        # Can be safely commented out according to JNM
        #assert controller_instance.submission_acceptable(mock_submission) == False

        ## Next, check acceptability on a more recent submission
        mock_submission.created_utc = int(datetime.datetime.now().timestamp())
        assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == 0
        assert controller_instance.submission_acceptable(mock_submission) == True

        ## Now check acceptability in a case where the identical comment exists
        ## First in the case where the comment is not stickied
        mock_submission.comments.append(treatment)
        assert controller_instance.submission_acceptable(mock_submission) == True

        ## And then in the case where the comment *is* stickied
        mock_submission.comments.append(stickied_treatment)
        assert controller_instance.submission_acceptable(mock_submission) == False

        mock_submission.comments = comments[0:-2]

        # Test outcome where intervention has already been recorded
        experiment_action = ExperimentAction(
            experiment_id = controller_instance.experiment.id,
            praw_key_id = None,
            action_subject_type = ThingType.COMMENT.value,
            action_subject_id = stickied_treatment.id,
            action = "Intervention",
            action_object_type = ThingType.SUBMISSION.value,
            action_object_id = submission.id,
            metadata_json = json.dumps({"group":"treatment", 
                "action_object_created_utc":int(time.time())})
        )
        db_session.add(experiment_action)
        db_session.commit()
        assert controller_instance.submission_acceptable(mock_submission) == False
        
        clear_all_tables()


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
@patch('praw.objects.Comment', autospec=True)
def test_make_sticky_post(mock_comment, mock_submission, mock_reddit):
    r = mock_reddit.return_value

    with open("{script_dir}/fixture_data/submission_0.json".format(script_dir=TEST_DIR)) as f:
        submission_json = json.loads(f.read())
        ## setting the submission time to be recent enough
        submission = json2obj(json.dumps(submission_json))
        mock_submission.id = submission.id
        mock_submission.json_dict = submission.json_dict
    
    with open("{script_dir}/fixture_data/submission_0_comments.json".format(script_dir=TEST_DIR)) as f:
        comments = json2obj(f.read())


    with open("{script_dir}/fixture_data/submission_0_treatment_distinguish.json".format(script_dir=TEST_DIR)) as f:
        distinguish = json.loads(f.read())
        mock_comment.distinguish.return_value = distinguish

    patch('praw.')


    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        # reload
        mock_submission.comments.return_value = comments


        with open("{script_dir}/fixture_data/submission_0_treatment.json".format(script_dir=TEST_DIR)) as f:
            treatment = json2obj(f.read())
            mock_comment.id = treatment.id
            mock_comment.created_utc = treatment.created_utc
            mock_submission.add_comment.return_value = mock_comment

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            block_name = "frontpage_post"
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:                
            block_name = "nonama"

        experiment_submission = ExperimentThing(
            id = submission.id,
            object_type = ThingType.SUBMISSION.value,
            experiment_id = controller_instance.experiment.id,
            metadata_json = json.dumps({"randomization":{"treatment":1, "block.id":"{0}.block001".format(block_name), "block.size":10}, "condition":"{0}".format(block_name)})            
        )


        ## Try to intervene
        mock_submission.created_utc = int(time.time())
        assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == 0
        sticky_result = controller_instance.make_sticky_post(experiment_submission, mock_submission)
        assert db_session.query(ExperimentAction).count() == 1
        assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == 1
        assert sticky_result is not None

        ## make sure it aborts the call if we try a second time
        sticky_result = controller_instance.make_sticky_post(experiment_submission, mock_submission)
        assert db_session.query(ExperimentAction).count() == 1
        assert sticky_result is None

        clear_all_tables()

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
@patch('praw.objects.Comment', autospec=True)
def test_make_control_nonaction(mock_comment, mock_submission, mock_reddit):
    r = mock_reddit.return_value

    with open("{script_dir}/fixture_data/submission_0.json".format(script_dir=TEST_DIR)) as f:
        submission_json = json.loads(f.read())
        ## setting the submission time to be recent enough
        submission = json2obj(json.dumps(submission_json))
        mock_submission.id = submission.id    

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:


        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            block_name = "frontpage_post"
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            block_name = "nonama"
        experiment_submission = ExperimentThing(
            id = submission.id,
            object_type = ThingType.SUBMISSION.value,
            experiment_id = controller_instance.experiment.id,
            metadata_json = json.dumps({"randomization":{
                                                    "treatment":0, 
                                                    "block.id":"{0}.block001".format(block_name), 
                                                    "block.size":10}, 
                                        "condition":"{0}".format(block_name)})            
            )

        mock_submission.created_utc = int(time.time())
        sticky_result = controller_instance.make_control_nonaction(experiment_submission, mock_submission, group="test")

        assert db_session.query(ExperimentAction).count() == 1
        assert sticky_result is not None
        action = db_session.query(ExperimentAction).first()
        action_metadata = json.loads(action.metadata_json)
        experiment_submission_metadata = json.loads(experiment_submission.metadata_json)
        assert action_metadata['group'] == "test"
        assert action_metadata['condition'] == experiment_submission_metadata['condition']
        assert action_metadata['arm'] == "arm_" + str(experiment_submission_metadata['randomization']['treatment'])
        assert 'randomization' in action_metadata.keys()

        ## make sure it aborts the call if we try a second time
        sticky_result = controller_instance.make_control_nonaction(experiment_submission, mock_submission)
        assert db_session.query(ExperimentAction).count() == 1
        assert sticky_result is None
        clear_all_tables()

@patch('praw.Reddit', autospec=True)
def test_find_treatment_replies(mock_reddit):
    fixture_dir = os.path.join(TEST_DIR, "fixture_data")

    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
        }

    for experiment_name in experiment_name_to_controller:
        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        experiment_start = parser.parse(experiment_settings['start_time'])

        with open(os.path.join(fixture_dir, "comment_tree_0.json"),"r") as f:
            comment_json = json.loads(f.read())
        
        for comment in comment_json:
            dbcomment = Comment(
                id = comment['id'],
                created_at = datetime.datetime.utcfromtimestamp(comment['created_utc']),
                #set fixture comments to experiment subreddit _id
                subreddit_id = experiment_settings['subreddit_id'], 
                post_id = comment['link_id'],
                user_id = comment['author'],
                comment_data = json.dumps(comment)
            )
            db_session.add(dbcomment)
        db_session.commit()

        comment_tree = Comment.get_comment_tree(db_session, sqlalchemyfilter = and_(Comment.subreddit_id == experiment_settings['subreddit_id']))
        treatment_comments = [(x.id, x.link_id, len(x.get_all_children()), x.data['created_utc']) for x in comment_tree['all_toplevel'].values()]
        
        experiment_submissions = []

        ## SET UP DATABASE ARCHIVE OF EXPERIMENT ACTIONS
        ## TO CORRESPOND TO THE FIXTURE DATA
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            block_name = "frontpage_post"
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            block_name = "nonama"

        for treatment_comment in treatment_comments:
            submission_id = treatment_comment[1].replace("t3","")
            if(submission_id not in experiment_submissions):
                experiment_submission = ExperimentThing(
                    id = submission_id,
                    object_type = ThingType.SUBMISSION.value,
                    experiment_id = controller_instance.experiment.id,
                    metadata_json = json.dumps({"randomization":{
                                                    "treatment":1, 
                                                    "block.id":"{0}.block001".format(block_name), 
                                                    "block.size":10}, 
                                                "condition":"{0}".format(block_name), 
                                                "condition":"{0}".format(block_name)})
                )
                db_session.add(experiment_submission)
                experiment_submissions.append(submission_id)

            experiment_comment = ExperimentThing(
                id = treatment_comment[0],
                object_type = ThingType.COMMENT.value,
                experiment_id = controller_instance.experiment.id,
                metadata_json = json.dumps({"group":"treatment",
                                            "arm":"arm_1",
                                            "condition":"{0}".format(block_name),
                                            "randomization":{
                                                    "treatment":1, 
                                                    "block.id":"{0}.block001".format(block_name), 
                                                    "block.size":10},
                                            "submission_id":submission_id})
            )
            db_session.add(experiment_comment)
            experiment_action = ExperimentAction(
                experiment_id = controller_instance.experiment.id,
                praw_key_id = None,
                action_subject_type = ThingType.COMMENT.value,
                action_subject_id = treatment_comment[0],
                action = "Intervention",
                action_object_type = ThingType.SUBMISSION.value,
                action_object_id = treatment_comment[1],
                metadata_json = json.dumps({"group":"treatment",
                                            "arm":"arm_1",
                                            "condition":"{0}".format(block_name),
                                            "randomization":{
                                                    "treatment":1, 
                                                    "block.id":"{0}.block001".format(block_name), 
                                                    "block.size":10},
                                            "action_object_created_utc":treatment_comment[3]})
            )
            db_session.add(experiment_action)
        db_session.commit()

        assert len(controller_instance.get_all_experiment_comments()) == len(treatment_comments)
        acre = controller_instance.get_all_experiment_comment_replies()

        ## NOW SET UP THE MOCK RETURN FROM: 
        ## get_comment_objects_for_experiment_comment_replies
        assert len(acre) == sum([x[2] for x in treatment_comments])
        return_comments = [json2obj(json.dumps(x.data)) for x in acre]
        r.get_info.return_value = return_comments
        
        ## NOW TEST THE REMOVAL OF THE COMMENTS
        assert db_session.query(ExperimentAction).filter(ExperimentAction.action=="RemoveRepliesToTreatment").count() == 0
        removed_count = controller_instance.remove_replies_to_treatments()
        experiment_action = db_session.query(ExperimentAction).filter(ExperimentAction.action=="RemoveRepliesToTreatment").first()
        assert removed_count == sum([x.banned_by is None for x in return_comments])
        removable_ids = [x.id for x in return_comments if x.banned_by is None]
        parent_ids = set([x.link_id for x in return_comments if x.banned_by is None])
        experiment_action_data = json.loads(experiment_action.metadata_json)
        for id in removable_ids:
            assert id in experiment_action_data['removed_comment_ids']
        for id in parent_ids:
            assert id in experiment_action_data['parent_submission_ids']

        
        ## NOW TEST THE REMOVAL OF COMMENTS WHEN THERE ARE NO COMMENTS TO REMOVE
        r.get_info.return_value = []
        removed_count = controller_instance.remove_replies_to_treatments()
        assert removed_count == 0
        
        clear_all_tables()        


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_identify_condition(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_ama_2020_test": AMA2020StickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
    }

    for experiment_name in experiment_name_to_controller:

        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        sub_data = []
        with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
            fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
            min_age = experiment_settings["min_eligibility_age"]
            for post in fixture:
                json_dump = json.dumps(post)
                postobj = json2obj(json_dump, now=True, offset=-1*min_age)
                sub_data.append(postobj)
        mock_subreddit.get_new.return_value = sub_data
        mock_subreddit.display_name = experiment_settings['subreddit']
        mock_subreddit.name = experiment_settings['subreddit']
        mock_subreddit.id = experiment_settings['subreddit_id']
        r.get_subreddit.return_value = mock_subreddit
        patch('praw.')

        ## TEST THE BASE CASE OF RANDOMIZATION
        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)

        # "mock" FrontPageController.posts
        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            instance = FrontPageController(db_session, r, log)
            instance.posts = sub_data


        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects(instance)
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()
        elif controller_instance.__class__ is AMA2020StickyCommentExperimentController:
            objs = controller_instance.set_eligible_objects()
            

        eligible_objects = controller_instance.get_eligible_objects(objs)

        condition_list = []
        for obj in eligible_objects:
            condition_list.append(controller_instance.identify_condition(obj))

        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            assert Counter(condition_list)['frontpage_post'] == 100
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            assert Counter(condition_list)['nonama'] == 98
            assert Counter(condition_list)['ama'] == 2
        elif controller_instance.__class__ is AMA2020StickyCommentExperimentController:
            assert Counter(condition_list)['nonama'] == 98
            assert Counter(condition_list)['ama'] == 2
            

        clear_all_tables()

@patch('praw.Reddit', autospec=True)
def test_frontpage_get_eligible_objects(mock_reddit):
    r = mock_reddit.return_value
    controller = FrontPageStickyCommentExperimentController
    controller_instance = controller("sticky_comment_frontpage_test", db_session, r, log)
    
    with open("{script_dir}/fixture_data/front_page_0.json".format(script_dir=TEST_DIR)) as f:
        fp_json = json.loads(f.read())['data']['children']
        ## setting the submission time to be recent enough
        mock_fp_posts = []
        min_age = controller_instance.min_eligibility_age
        for post in fp_json:
            mock_post = json2obj(json.dumps(post['data']), now=True, offset=-1*min_age)
            mock_fp_posts.append(mock_post)

    eligible_objects = controller_instance.get_eligible_objects(mock_fp_posts)
    assert len(eligible_objects) == 6
    for obj in eligible_objects:
        assert "t5_" + controller_instance.subreddit_id == obj.subreddit_id

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
@patch('praw.objects.Comment', autospec=True)
def test_archive_experiment_submission_metadata(mock_comment, mock_submission, mock_reddit):
    r = mock_reddit.return_value

    experiment_name_to_controller = {
        "sticky_comment_0": AMAStickyCommentExperimentController,
        "sticky_comment_frontpage_test": FrontPageStickyCommentExperimentController
    }

    for experiment_name in experiment_name_to_controller:

    
        with open(os.path.join(BASE_DIR, "config", "experiments") + "/"+ experiment_name + ".yml", "r") as f:
            experiment_settings = yaml.load(f.read())['test']

        with open("{script_dir}/fixture_data/submission_0.json".format(script_dir=TEST_DIR)) as f:
            submission_json = json.loads(f.read())
            ## setting the submission time to be recent enough
            submission = json2obj(json.dumps(submission_json))
            mock_submission.id = submission.id
            mock_submission.json_dict = submission.json_dict
        
        with open("{script_dir}/fixture_data/submission_0_comments.json".format(script_dir=TEST_DIR)) as f:
            comments = json2obj(f.read())
            mock_submission.comments.return_value = comments

        controller = experiment_name_to_controller[experiment_name]
        controller_instance = controller(experiment_name, db_session, r, log)


        with open("{script_dir}/fixture_data/submission_0_treatment.json".format(script_dir=TEST_DIR)) as f:
            treatment = json2obj(f.read())
            mock_comment.id = treatment.id
            mock_comment.created_utc = treatment.created_utc
            mock_submission.add_comment.return_value = mock_comment

        # don't have to customize???
        with open("{script_dir}/fixture_data/submission_0_treatment_distinguish.json".format(script_dir=TEST_DIR)) as f:
            distinguish = json.loads(f.read())
            mock_comment.distinguish.return_value = distinguish

        patch('praw.')


        if controller_instance.__class__ is FrontPageStickyCommentExperimentController:
            block_name = "frontpage_post"
        elif controller_instance.__class__ is AMAStickyCommentExperimentController:
            block_name = "nonama"


        experiment_submission = ExperimentThing(
            id = submission.id,
            object_type = ThingType.SUBMISSION.value,
            experiment_id = controller_instance.experiment.id,
            metadata_json = json.dumps({"randomization":{"treatment":1, "block.id":"{0}.block001".format(block_name), "block.size":10}, "condition":"{0}".format(block_name)})            
        )
        db_session.add(experiment_submission)
        db_session.commit()

        ## intervene
        mock_submission.created_utc = int(time.time())
        assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == 0
        sticky_result = controller_instance.make_sticky_post(experiment_submission, mock_submission)
        assert db_session.query(ExperimentAction).count() == 1
        assert db_session.query(ExperimentThing).filter(ExperimentThing.object_type==ThingType.COMMENT.value).count() == 1
        assert sticky_result is not None

        ## TEST archive_experiment_submission_metadata
        r.get_info.return_value = [submission]
        snapshots = controller_instance.archive_experiment_submission_metadata()
        assert len(snapshots) == 1
        assert db_session.query(ExperimentThingSnapshot).count()
        ets = db_session.query(ExperimentThingSnapshot).first()
        metadata = json.loads(ets.metadata_json)
        assert metadata['num_reports']  == 0
        assert metadata['user_reports']  == 0
        assert metadata['mod_reports']  == 0
        assert metadata['score']        == 1
        assert metadata['num_comments'] == 3
        assert ets.experiment_id        == controller_instance.experiment.id
        assert ets.experiment_thing_id  == submission.id
        assert ets.object_type          == ThingType.SUBMISSION.value

        clear_all_tables()
