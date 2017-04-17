import inspect, os, sys, yaml
import simplejson as json
import reddit.connection
import lumen_connect.connection
import twitter_connect.connection
import app.controllers.front_page_controller
import app.controllers.subreddit_controller
import app.controllers.comment_controller
import app.controllers.moderator_controller
import app.controllers.sticky_comment_experiment_controller
import app.controllers.lumen_controller
import app.controllers.twitter_controller
from utils.common import PageType, DbEngine
import app.cs_logger
from app.models import Base, SubredditPage, Subreddit, Post, ModAction, Experiment
import datetime

### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
ENV = os.environ['CS_ENV']

### LOAD SQLALCHEMY SESSION
db_session = DbEngine(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV)).new_session()

# LOAD LOGGER
log = app.cs_logger.get_logger(ENV, BASE_DIR)

conn = reddit.connection.Connect()
lumen_conn = lumen_connect.connection.LumenConnect(log)
twitter_conn = twitter_connect.connection.TwitterConnect(log)

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
    subreddit_id = db_session.query(Subreddit).filter(Subreddit.name == subreddit).first().id

    first_action_count = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count()
    log.info("Fetching Moderation Action History for {subreddit}. {n} actions are currently in the archive.".format(
        subreddit = subreddit,
        n = first_action_count))
    after_id = mac.archive_mod_action_page(after_id)
    db_session.commit()
    num_actions_stored = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count() - first_action_count

    while(num_actions_stored > 0):
        pre_action_count = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count()
        after_id = mac.archive_mod_action_page(after_id)
        db_session.commit()
        num_actions_stored = db_session.query(ModAction).filter(ModAction.subreddit_id == subreddit_id).count() - pre_action_count
   
    log.info("Finished Fetching Moderation Action History for {subreddit}. {stored} actions were stored, with a total of {total}.".format(
        subreddit = subreddit,
        stored = pre_action_count - first_action_count,
        total = pre_action_count))

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
            self.log.error("Failure loading experiment yaml {0}".format(experiment_file_path), str(exc))
            sys.exit(1)

    if(ENV not in experiment_config_all.keys()):
        self.log.error("Cannot find experiment settings for {0} in {1}".format(ENV, experiment_file_path))
        sys.exit(1)
    experiment_config = experiment_config_all[ENV]
    c = getattr(app.controllers.sticky_comment_experiment_controller, experiment_config['controller'])
    return c


def conduct_sticky_comment_experiment(experiment_name):
    c = get_experiment_class(experiment_name) 
    r = conn.connect(controller=experiment_name)    
    sce = c(        
        experiment_name = experiment_name,
        db_session = db_session,
        r = r,
        log = log
    )
    sce.update_experiment()

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
    sce = c(
        experiment_name = experiment_name,
        db_session = db_session,
        r = r,
        log = log
    )
    sce.archive_experiment_submission_metadata()
  

def archive_lumen_notices():
    l = app.controllers.lumen_controller.LumenController(db_session, lumen_conn, twitter_conn, log)

    topics = ["Copyright"] # "Government Requests", #["Defamation","Protest, Parody and Criticism Sites","Law Enforcement Requests","International","Government Requests","DMCA Subpoenas","Court Orders"]
    date = datetime.datetime.utcnow() - datetime.timedelta(days=2) # now-1day
    l.archive_lumen_notices(topics, date)

# TODO: confirm args?
def archive_twitter_users(users_file):
    t = app.controllers.twitter_controller.TwitterController(db_session, twitter_conn, log)
    with open(users_file) as f:
         users = json.loads(f.read())
    t.archive_users(users)

# TODO: confirm args?
def archive_user_tweets(username):
    t = app.controllers.twitter_controller.TwitterController(db_session, twitter_conn, log)
    t.archive_user_tweets(username)    