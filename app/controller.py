import inspect, os, sys, yaml
### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
ENV = os.environ['CS_ENV']

sys.path.append(BASE_DIR)

import simplejson as json
import reddit.connection
import datetime
import app.controllers.front_page_controller
import app.controllers.subreddit_controller
import app.controllers.comment_controller
import app.controllers.moderator_controller
import app.controllers.stylesheet_experiment_controller
import app.controllers.sticky_comment_experiment_controller
import app.controllers.mod_user_experiment_controller
from utils.common import PageType, DbEngine
import app.cs_logger
from app.models import Base, SubredditPage, Subreddit, Post, ModAction, Experiment



### LOAD SQLALCHEMY SESSION
db_session = DbEngine(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV)).new_session()

# LOAD LOGGER
log = app.cs_logger.get_logger(ENV, BASE_DIR)

conn = reddit.connection.Connect()

def fetch_reddit_front(page_type=PageType.TOP):
    r = conn.connect(controller="FetchRedditFront")
    fp = app.controllers.front_page_controller.FrontPageController(db_session, r, log)
    fp.archive_reddit_front_page(page_type)

def fetch_subreddit_front(sub_name, page_type = PageType.TOP):
    r = conn.connect(controller="FetchSubredditFront")
    sp = app.controllers.subreddit_controller.SubredditPageController(sub_name, db_session, r, log)
    sp.archive_subreddit_page(pg_type = page_type)

def fetch_post_comments(post_id):
    r = conn.connect(controller="FetchComments")
    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    cc.archive_missing_post_comments(post_id)

def fetch_missing_subreddit_post_comments(subreddit_id):
    r = conn.connect(controller="FetchComments")
    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    cc.archive_all_missing_subreddit_post_comments(subreddit_id)

def fetch_mod_action_history(subreddit, after_id = None):
    r = conn.connect(controller="ModLog")
    mac = app.controllers.moderator_controller.ModeratorController(subreddit, db_session, r, log)
    mac.archive_mod_action_history(after_id)

# run as a job regularly, when running mod_action_experiment
### 3) run controller.py job: fetch_new_mod_actions(shadow_subreddit_name). 
###    this calls event_hooks
def fetch_new_mod_actions(subreddit):
    r = conn.connect(controller="ModLog")
    mac = app.controllers.moderator_controller.ModeratorController(subreddit, db_session, r, log)
    mac.archive_new_mod_actions()

#def fetch_mod_action_history(subreddit, after_id = None):
#    r = conn.connect(controller="ModLog")
#    mac = app.controllers.moderator_controller.ModeratorController(subreddit, db_session, r, log)
#    subreddit_id = db_session.query(Subreddit).filter(Subreddit.name == subreddit).first().id
#
#    first_action_count = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count()
#    log.info("Fetching Moderation Action History for {subreddit}. {n} actions are currently in the archive.".format(
#        subreddit = subreddit,
#        n = first_action_count))
#    after_id = mac.archive_mod_action_page(after_id)
#    db_session.commit()
#    num_actions_stored = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count() - first_action_count
#
#    while(num_actions_stored > 0):
#        pre_action_count = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count()
#        after_id = mac.archive_mod_action_page(after_id)
#        db_session.commit()
#        num_actions_stored = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count() - pre_action_count
#   
#    log.info("Finished Fetching Moderation Action History for {subreddit}. {stored} actions were stored, with a total of {total}.".format(
#        subreddit = subreddit,
#        stored = pre_action_count - first_action_count,
#        total = pre_action_count))

def fetch_last_thousand_comments(subreddit_name):
    r = conn.connect(controller="FetchComments")
    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    cc.archive_last_thousand_comments(subreddit_name)

def get_experiment_class(experiment_name):
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

    experiment_controllers = [app.controllers.stylesheet_experiment_controller, app.controllers.sticky_comment_experiment_controller, app.controllers.mod_user_experiment_controller]
    for experiment_controller in experiment_controllers:
        try:
            c = getattr(experiment_controller, experiment_config['controller'])
            return c
        except AttributeError:
            continue

    log.error("Cannot find experiment controller for {0}".format(experiment_config['controller']))
    return None


# for sticky comment experiments that are *NOT* using event_handler+callbacks
def conduct_sticky_comment_experiment(experiment_name):
    sce = initialize_sticky_comment_experiment(experiment_name)
    if sce:
        sce.update_experiment()
    else:
        log.error("conduct_sticky_comment_experiment for experiment name {0} failed.".format(experiment_name))

# legacy. use initialize_experiment instead
def initialize_sticky_comment_experiment(experiment_name):
    return initialize_experiment(experiment_name)

# not to be run as a job, just to store and get a sce object
### 1) controller.py: initialize_experiment("mod_user_test"). 
###    store Experiment record
def initialize_experiment(experiment_name):
    r = conn.connect(controller=experiment_name)    
    c = get_experiment_class(experiment_name) 
    if c:
        sce = c(        
            experiment_name = experiment_name,
            db_session = db_session,
            r = r,
            log = log
        )
        return sce
    else:
        log.error("initialize_experiment for experiment name {0} failed.".format(experiment_name))

# not to be run as a job, just to store initial banned users in main subreddit in user_metadata
# only updates/stores UserMetadata according to ModAction records with created_utc >= oldest_mod_action_created_utc 
# class must have method pre
# should only run pre() once
### 2) controller.py: pre_experiment("mod_user_test")
###    load mod action history of main_subreddit
###    store UserMetadata records
def pre_experiment(experiment_name):
    ## expecting string like "2017-06-21 14:40:41", which is what you get from mysql output
    #oldest_mod_action_created_utc = None
    #if oldest_mod_action_created_utc_str:     
    #    oldest_mod_action_created_utc = datetime.datetime.strptime(oldest_mod_action_created_utc_str, "%Y-%m-%d %H:%M:%S")

    r = conn.connect(controller=experiment_name)    
    c = get_experiment_class(experiment_name) 
    if c:
        sce = c(        
            experiment_name = experiment_name,
            db_session = db_session,
            r = r,
            log = log
        )
        sce.pre()
    else:
        log.error("fetch_initial_banned_users for experiment name {0} failed.".format(experiment_name))


#######################
def get_mod_user_experiment_controller(experiment_name):
    r = conn.connect(controller="ModLog")
    mac = app.controllers.mod_user_experiment_controller.ModUserExperimentController(experiment_name, db_session, r, log)
    return mac
#######################

def remove_experiment_replies(experiment_name):
    r = conn.connect(controller=experiment_name)    
    sce = app.controllers.sticky_comment_experiment_controller.StickyCommentExperimentController(
        experiment_name = experiment_name,
        db_session = db_session,
        r = r,
        log = log
    )
    sce.remove_replies_to_treatments()

def archive_experiment_submission_metadata(experiment_name):
    r = conn.connect(controller=experiment_name)
    c = get_experiment_class(experiment_name)
    if c:
        sce = c(
            experiment_name = experiment_name,
            db_session = db_session,
            r = r,
            log = log
        )
        sce.archive_experiment_submission_metadata()
    else:
        log.error("initialize_experiment for experiment name {0} failed.".format(experiment_name))


def update_stylesheet_experiment(experiment_name):
    r = conn.connect(controller=app.controllers.stylesheet_experiment_controller.StylesheetExperimentController)
    sce = app.controllers.stylesheet_experiment_controller.StylesheetExperimentController(
        experiment_name = experiment_name,
        db_session = db_session,
        r = r,
        log = log
    )
    sce.update_experiment()

if __name__ == "__main__":
    fnc = sys.argv[1]
    args =  sys.argv[2:]
    locals()[fnc](*args)
