import inspect, os, sys, yaml
### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
ENV = os.environ['CS_ENV']

sys.path.append(BASE_DIR)

import simplejson as json
import app.connections.reddit_connect
import app.connections.lumen_connect
import app.connections.twitter_connect
import app.controllers.front_page_controller
import app.controllers.subreddit_controller
import app.controllers.comment_controller
import app.controllers.moderator_controller
import app.controllers.stylesheet_experiment_controller
import app.controllers.sticky_comment_experiment_controller
import app.controllers.lumen_controller
import app.controllers.twitter_controller
import app.controllers.twitter_observational_analysis_controller
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

conn = app.connections.reddit_connect.RedditConnect()
lumen_conn = app.connections.lumen_connect.LumenConnect(log)
twitter_conn = app.connections.twitter_connect.TwitterConnect(log=log, db_session = db_session)

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
            log.error("Failure loading experiment yaml {0}".format(experiment_file_path), str(exc))
            sys.exit(1)

    if(ENV not in experiment_config_all.keys()):
        log.error("Cannot find experiment settings for {0} in {1}".format(ENV, experiment_file_path))
        sys.exit(1)
    experiment_config = experiment_config_all[ENV]

    ## this is a hack. needs to be improved
    if(experiment_config['controller'] == "StylesheetExperimentController"):
        c = getattr(app.controllers.stylesheet_experiment_controller, experiment_config['controller'])
    else:
        c = getattr(app.controllers.sticky_comment_experiment_controller, experiment_config['controller'])
    return c


# for sticky comment experiments that are NOT using event_handler+callbacks
def conduct_sticky_comment_experiment(experiment_name):
    sce = initialize_sticky_comment_experiment(experiment_name)
    sce.update_experiment()

# not to be run as a job, just to store and get a sce object
def initialize_sticky_comment_experiment(experiment_name):
    c = get_experiment_class(experiment_name)
    r = conn.connect(controller=experiment_name)
    sce = c(
        experiment_name = experiment_name,
        db_session = db_session,
        r = r,
        log = log
    )
    return sce

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

def update_stylesheet_experiment(experiment_name):
    r = conn.connect(controller=app.controllers.stylesheet_experiment_controller.StylesheetExperimentController)
    sce = app.controllers.stylesheet_experiment_controller.StylesheetExperimentController(
        experiment_name = experiment_name,
        db_session = db_session,
        r = r,
        log = log
    )
    sce.update_experiment()


def fetch_lumen_notices(num_days=2):
    """
    Archive lumen notices.
    """
    log.info("Calling fetch_lumen_notices, num_days={0}, PID={1}".format(num_days, str(os.getpid())))
    l = app.controllers.lumen_controller.LumenController(db_session, lumen_conn, log)

    topics = ["Copyright"]    # "Government Requests", #["Defamation","Protest, Parody and Criticism Sites","Law Enforcement Requests","International","Government Requests","DMCA Subpoenas","Court Orders"]
    date = datetime.datetime.utcnow() - datetime.timedelta(days=int(float(num_days))) # now-2days
    l.archive_lumen_notices(topics, date)
    log.info("Finished fetch_lumen_notices, num_days={0}, PID={1}".format(num_days, str(os.getpid())))


def parse_lumen_notices_for_twitter_accounts():
    """
    For all LumenNotices with CS_parsed_usernames=False, parse for twitter accounts
    """
    log.info("Calling parse_lumen_notices_for_twitter_accounts. PID={0}".format(str(os.getpid())))
    l = app.controllers.lumen_controller.LumenController(db_session, lumen_conn, log)
    l.query_and_parse_notices_archive_users()
    log.info("Finished parse_lumen_notices_for_twitter_accounts. PID={0}".format(str(os.getpid())))


def fetch_twitter_users():
    """
    For all LumenNoticeToTwitterUser with CS_account_queried=False,
    archive Twitter accounts in TwitterUser objects,  and create 1st TwitterUserSnapshot
    """
    log.info("Calling fetch_twitter_users. PID={0}".format(str(os.getpid())))
    t = app.controllers.twitter_controller.TwitterController(db_session, twitter_conn, log)
    t.query_and_archive_new_users()
    twitter_conn.checkin_endpoint()
    log.info("Finished fetch_twitter_users. PID={0}".format(str(os.getpid())))


def fetch_twitter_snapshot_and_tweets(max_time_delta_min=60):
    """
    For all TwitterUserSnapshot.created_at older than x min, fetch another snapshot
    """
    log.info("Calling fetch_twitter_snapshot_and_tweets, max_time_delta_min={0}".format(max_time_delta_min))
    t = app.controllers.twitter_controller.TwitterController(db_session, twitter_conn, log)
    now = datetime.datetime.utcnow()
    date = now - datetime.timedelta(minutes=int(float(max_time_delta_min))) # now-1hour
    t.query_and_archive_user_snapshots_and_tweets(date)
    twitter_conn.checkin_endpoint()
    log.info("Finished fetch_twitter_snapshot_and_tweets, max_time_delta_min={0}".format(max_time_delta_min))


def fetch_twitter_tweets(backfill=False, fill_start_time=None):
    """
    For all TwitterUsers with CS_most_tweets_queried=False, fetch tweets
    """
    log.info("Calling fetch_twitter_tweets, backfill={0}.".format(backfill))
    t = app.controllers.twitter_controller.TwitterController(db_session, twitter_conn, log)
    t.query_and_archive_tweets(backfill, fill_start_time=fill_start_time)
    twitter_conn.checkin_endpoint()
    log.info("Finished fetch_twitter_tweets, backfill={0}.".format(backfill))


def twitter_observational_analysis_basic_profiling():
    tb = app.controllers.twitter_observational_analysis_controller.TwitterBasicProfilingController(
        "/home/mmou/Dropbox/Documents/Chronos/MIT/CM/CivilServant", db_session, log)
    tb.basic_profiling_create_dataset()


# python app/controller.py twitter_observational_analysis 2017-05-31 2017-06-02 7 /home/mmou/Dropbox/Documents/Chronos/MIT/CM/CivilServant
def twitter_observational_analysis(start_date, end_date, min_observed_days, output_dir):
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    min_observed_days = int(min_observed_days)

    to = app.controllers.twitter_observational_analysis_controller.TwitterObservationalAnalysisController(
        start_date, end_date, min_observed_days, output_dir, db_session, log)
    to.create_csvs()


if __name__ == "__main__":
    fnc = sys.argv[1]
    args =  sys.argv[2:]
    locals()[fnc](*args)
