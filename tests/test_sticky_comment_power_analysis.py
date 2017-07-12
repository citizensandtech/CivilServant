import reddit.connection
import reddit.queries
import os, glob, praw
from mock import Mock, patch
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
import app.cs_logger
from app.models import Base, PrawKey, Subreddit
from app.controllers.sticky_comment_power_analysis_controller import *
from dateutil import parser
from utils.common import DbEngine
from collections import Counter 

TEST_DIR = os.path.dirname(os.path.realpath(__file__))

os.environ['CS_ENV'] ="test"

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
ENV = os.environ['CS_ENV'] ="test"

log = app.cs_logger.get_logger(ENV, os.path.join(TEST_DIR,"../"))
db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()

def clear_database():
    ## REMOVE DATABASE ENTRIES
    db_session.query(Subreddit).delete()
    db_session.query(ModAction).delete()
    db_session.commit()

def setup_database():
  ## LOAD SUBREDDITS FROM FIXTURES
  with open(os.path.join(TEST_DIR, "fixture_data", "subreddits.json"), "r") as f:
    for subreddit in json.loads(f.read()):
      sub = Subreddit(id=subreddit['id'],
                      name = subreddit['name'])
      db_session.add(sub)
  db_session.commit()

  with open (os.path.join(TEST_DIR, "fixture_data", "mod_actions_1.json"), "r") as f:
    mod_action_json = json.loads(f.read())
    for mod_action in mod_action_json:
      ma_db = ModAction(
        id = mod_action['id'],
        created_utc = datetime.datetime.utcfromtimestamp(mod_action['created_utc']),
        subreddit_id = mod_action['sr_id36'],
        target_author = mod_action['target_author'],
        target_fullname = mod_action['target_fullname'],
        mod = mod_action['mod_id36'],
        action = mod_action['action'],
        action_data = json.dumps(mod_action)
      )
      db_session.add(ma_db)
  db_session.commit()


def setup_function(function):
  clear_database()
  setup_database()


def teardown_function(function):
    ## REMOVE OUTPUT FILES
    files = glob.glob(os.path.join(TEST_DIR, "fixture_data", "outputs", "*"))
    for f in files:
      os.remove(f)

    clear_database()



def test_init():
  controller = StickyCommentPowerAnalysisController("2qh16", 
                 parser.parse("2017-01-01"), 
                 parser.parse("2017-01-02"),
                 os.path.join(TEST_DIR, "fixture_data", "reddit_archive"),
                 os.path.join(TEST_DIR, "outputs"), db_session, log)
  assert controller.start_date_utc == utc.localize(parser.parse("2017-01-01"))
  assert controller.end_date_utc == utc.localize(parser.parse("2017-01-02"))

def test_get_posts():
  controller = StickyCommentPowerAnalysisController("2qh16", 
                 parser.parse("2017-01-01"), 
                 parser.parse("2017-01-02"),
                 os.path.join(TEST_DIR, "fixture_data", "reddit_archive"),
                 os.path.join(TEST_DIR, "outputs"), db_session, log)
  posts = controller.get_posts()
  assert len(posts) == 838

  first_post = list(posts.values())[0]
  key_missing = False
  for key in ['author.prev.posts', 'is.selftext', 'author.prev.participation', 'num.comments.removed', 'created.utc', 'newcomer.comments.removed', 'visible', 'newcomer.comments', 'front_page', 'weekday', 'body.length', 'author', 'url', 'num.comments', 'author.deleted.later']:
    if(key not in first_post.keys()):
      key_missing = True
  assert key_missing == False



  ## TODO: ADJUST THE FIXTURES SO THEY MOSTLY HAVE POSTS FROM THAT SUBREDDIT
  ## AND THEN ADD A COMMENTS FIXTURE
  ## THIS SHOULD MAKE THE PROCESS MORE EFFICIENT


def test_get_comments():
  controller = StickyCommentPowerAnalysisController("2qh16", 
                 parser.parse("2017-01-01"), 
                 parser.parse("2017-01-02"),
                 os.path.join(TEST_DIR, "fixture_data", "reddit_archive"),
                 os.path.join(TEST_DIR, "outputs"), db_session, log)
  comments = controller.get_comments()
  assert len(comments) == 3259

def test_get_modlog():
  controller = StickyCommentPowerAnalysisController("mouw", 
                 parser.parse("2017-01-01"), 
                 parser.parse("2017-01-02"),
                 os.path.join(TEST_DIR, "fixture_data", "reddit_archive"),
                 os.path.join(TEST_DIR, "outputs"), db_session, log)
  (mod_actions_comments, mod_actions_posts) = controller.get_modlog()
  assert len(mod_actions_comments) == 383
  assert len(mod_actions_posts) == 56

def test_get_post_to_comment_info():
  ## TODO: SET UP THE TEST SO THAT THERE ARE COMMENTS
  ## THAT HAVE BEEN REMOVED
  controller = StickyCommentPowerAnalysisController("2qh16", 
                 parser.parse("2017-01-01"), 
                 parser.parse("2017-01-02"),
                 os.path.join(TEST_DIR, "fixture_data", "reddit_archive"),
                 os.path.join(TEST_DIR, "outputs"), db_session, log)

  controller.comments = controller.get_comments()
  controller.posts = controller.get_posts()
  controller.post_to_comment_info = controller.get_post_to_comment_info()
  assert len(controller.post_to_comment_info) == 40
  assert len(controller.post_to_comment_info['5ng4mg']['comments']) == 14

def test_apply_participation_and_post_to_comment_info():
  ## TODO: SET UP THE TEST SO THAT THERE ARE COMMENTS
  ## THAT HAVE BEEN REMOVED
  controller = StickyCommentPowerAnalysisController("2qh16",
                 parser.parse("2017-01-01"),
                 parser.parse("2017-01-02"),
                 os.path.join(TEST_DIR, "fixture_data", "reddit_archive"),
                 os.path.join(TEST_DIR, "outputs"), db_session, log)

  controller.comments = controller.get_comments()
  controller.posts = controller.get_posts()
  (controller.mod_actions_comments, controller.mod_actions_posts) = controller.get_modlog()
  controller.post_to_comment_info = controller.get_post_to_comment_info()

  controller.apply_participation_and_post_to_comment_info()
  counter = Counter([x['newcomer.comments']>0 for x in list(controller.posts.values())])
  assert counter[True] == 30
