
from app.models import *
from utils.common import *
import app.cs_logger

from datetime import datetime, timedelta
from mock import Mock, patch
import importlib
import os
import pytest
import random

ENV = os.environ["CS_ENV"] = "test"
import utils.email_db_report

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")

END_DT = datetime(2017,12,31)
DAYS = 7

get_count = lambda: random.choice(range(5,15))
PAGES_PER_DAY = get_count()
POSTS_PER_DAY = get_count()
COMMENTS_PER_POST = get_count()
USERS_PER_DAY = get_count()
MOD_ACTIONS_PER_DAY = get_count()
EXPERIMENTS_PER_DAY = get_count()
THINGS_PER_EXPERIMENT = get_count()
SNAPSHOTS_PER_THING = get_count()
ACTIONS_PER_EXPERIMENT = get_count()

SUBREDDIT_ID = "mouw"
SUBREDDIT_NAME = "science"

db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)
experiment_test_info = {}

def get_experiment_test_info(day, num):
    name = "%d_%d" % (day, num)
    return experiment_test_info[name], name

def clear_all_tables():
    for table in reversed(Base.metadata.sorted_tables):
        db_session.execute(table.delete())
    db_session.commit()

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()
    experiment_test_info = {}

@pytest.fixture
def init_front_pages():
    assert len(db_session.query(FrontPage).all()) == 0

    for day in range(DAYS):
        for _ in range(PAGES_PER_DAY):
            for page_type in PageType:
                db_session.add(FrontPage(
                    created_at = END_DT - timedelta(days=day),
                    page_type = page_type.value,
                    is_utc = True))

    db_session.commit()

@pytest.fixture
def init_subreddit_pages(init_subreddits):
    assert len(db_session.query(Subreddit).all()) == 1
    assert len(db_session.query(SubredditPage).all()) == 0

    for day in range(DAYS):
        for _ in range(PAGES_PER_DAY):
            for page_type in PageType:
                db_session.add(SubredditPage(
                    created_at = END_DT - timedelta(days=day),
                    page_type = page_type.value,
                    subreddit_id = SUBREDDIT_ID,
                    is_utc = True))
    
    db_session.commit()

@pytest.fixture
def init_subreddits():
    assert len(db_session.query(Subreddit).all()) == 0
    
    db_session.add(Subreddit(
        id = SUBREDDIT_ID,
        name = SUBREDDIT_NAME,
        created_at = END_DT - timedelta(days=DAYS)))
    db_session.commit()

@pytest.fixture
def init_posts(init_subreddits):
    assert len(db_session.query(Subreddit).all()) == 1
    assert len(db_session.query(Post).all()) == 0
    
    for day in range(DAYS):
        for post in range(POSTS_PER_DAY):
            db_session.add(Post(
                id = "%s_%d_%d" % (SUBREDDIT_ID, day, post),
                created_at = END_DT - timedelta(days=day),
                subreddit_id = SUBREDDIT_ID))

    db_session.commit()

@pytest.fixture
def init_comments(init_posts):
    assert len(db_session.query(Subreddit).all()) == 1
    assert len(db_session.query(Post).all()) == DAYS * POSTS_PER_DAY
    assert len(db_session.query(Comment).all()) == 0
    
    for day in range(DAYS):
        for post in range(POSTS_PER_DAY):
            post_id = "%s_%d_%d" % (SUBREDDIT_ID, day, post)
            for comment in range(COMMENTS_PER_POST):
                db_session.add(Comment(
                    id = "%s_%s" % (post_id, comment),
                    created_at = END_DT - timedelta(days=day),
                    subreddit_id = SUBREDDIT_ID,
                    post_id = post_id))

    db_session.commit()

@pytest.fixture
def init_users():
    assert len(db_session.query(User).all()) == 0

    for day in range(DAYS):
        for user in range(USERS_PER_DAY):
            dt = END_DT - timedelta(days=day)
            db_session.add(User(
                name = "user_%d_%d" % (day, user),
                created = dt,
                first_seen = dt,
                last_seen = dt))
        
    db_session.commit()

@pytest.fixture
def init_mod_actions(init_subreddits):
    assert len(db_session.query(Subreddit).all()) == 1
    assert len(db_session.query(ModAction).all()) == 0

    for day in range(DAYS):
        for mod_action in range(MOD_ACTIONS_PER_DAY):
            db_session.add(ModAction(
                id = "%s_%d_%d" % (SUBREDDIT_ID, day, mod_action),
                created_at = END_DT - timedelta(days=day),
                subreddit_id = SUBREDDIT_ID))

    db_session.commit()

@pytest.fixture
def init_experiments():
    assert len(db_session.query(Experiment).all()) == 0

    for day in range(DAYS):
        for experiment in range(EXPERIMENTS_PER_DAY):
            dt = END_DT - timedelta(days=day)
            experiment = Experiment(
                name = "%d_%d" % (day, experiment),
                controller = "dummy_controller",
                created_at = dt,
                start_time = dt,
                end_time = dt)
            db_session.add(experiment)
            db_session.flush()
            experiment_test_info[experiment.name] = experiment.id

    db_session.commit()

@pytest.fixture
def init_experiment_things(init_experiments):
    assert len(db_session.query(Experiment).all()) == DAYS * EXPERIMENTS_PER_DAY
    assert len(db_session.query(ExperimentThing).all()) == 0

    for day in range(DAYS):
        for experiment in range(EXPERIMENTS_PER_DAY):
            experiment_id, experiment_name = get_experiment_test_info(day, experiment)
            for thing in range(THINGS_PER_EXPERIMENT):
                db_session.add(ExperimentThing(
                    id = "%s_%d" % (experiment_name, thing),
                    created_at = END_DT - timedelta(days=day),
                    object_type = ThingType.SUBMISSION.value,
                    experiment_id = experiment_id))

    db_session.commit()

@pytest.fixture
def init_experiment_thing_snapshots(init_experiment_things):
    thing_count = DAYS * EXPERIMENTS_PER_DAY * THINGS_PER_EXPERIMENT
    assert len(db_session.query(Experiment).all()) == DAYS * EXPERIMENTS_PER_DAY
    assert len(db_session.query(ExperimentThing).all()) == thing_count
    assert len(db_session.query(ExperimentThingSnapshot).all()) == 0

    for day in range(DAYS):
        for experiment in range(EXPERIMENTS_PER_DAY):
            experiment_id, experiment_name = get_experiment_test_info(day, experiment)
            for thing in range(THINGS_PER_EXPERIMENT):
                experiment_thing_id = "%s_%d" % (experiment_name, thing)
                for snapshot in range(SNAPSHOTS_PER_THING):
                    db_session.add(ExperimentThingSnapshot(
                        experiment_thing_id = experiment_thing_id,
                        created_at = END_DT - timedelta(days=day),
                        object_type = ThingType.SUBMISSION.value,
                        experiment_id = experiment_id))

    db_session.commit()

@pytest.fixture
def init_experiment_actions(init_experiments):
    assert len(db_session.query(Experiment).all()) == DAYS * EXPERIMENTS_PER_DAY
    assert len(db_session.query(ExperimentAction).all()) == 0

    for day in range(DAYS):
        for experiment in range(EXPERIMENTS_PER_DAY):
            experiment_id, _ = get_experiment_test_info(day, experiment)
            for action in range(ACTIONS_PER_EXPERIMENT):
                db_session.add(ExperimentAction(
                    action = "test",
                    created_at = END_DT - timedelta(days=day),
                    experiment_id = experiment_id))

    db_session.commit()


def test_generate_reddit_front_page(init_front_pages):
    assert len(db_session.query(FrontPage).all()) == DAYS * PAGES_PER_DAY * len(PageType)
    
    report = importlib.reload(utils.email_db_report)
    output = set(report.generate_reddit_front_page(END_DT, DAYS, html=False))
    assert len(output) == DAYS * len(PageType)

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        for page_type in PageType:
            record = (page_type.name, dt.year, dt.month, dt.day, PAGES_PER_DAY)
            assert record in output

def test_generate_reddit_subreddit_page(init_subreddit_pages):
    assert len(db_session.query(SubredditPage).all()) == DAYS * PAGES_PER_DAY * len(PageType)

    report = importlib.reload(utils.email_db_report)
    output = set(report.generate_reddit_subreddit_page(END_DT, DAYS, html=False))
    assert len(output) == DAYS * len(PageType)

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        for page_type in PageType:
            label = "(%s, %s)" % (SUBREDDIT_NAME, page_type.name)
            record = (label, dt.year, dt.month, dt.day, PAGES_PER_DAY)
            assert record in output

def test_generate_reddit_subreddit(init_subreddits):
    assert len(db_session.query(Subreddit).all()) == 1

    report = importlib.reload(utils.email_db_report)
    output = report.generate_reddit_subreddit(END_DT, DAYS, html=False)    
    assert len(output) == 1

    dt = END_DT - timedelta(days=DAYS)
    assert output[0][1:] == (dt.year, dt.month, dt.day, 1)

def test_generate_reddit_post(init_posts):
    assert len(db_session.query(Post).all()) == DAYS * POSTS_PER_DAY

    report = importlib.reload(utils.email_db_report)
    output = {tuple(item) for item in report.generate_reddit_post(END_DT, DAYS, html=False)}
    assert len(output) == DAYS

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        record = (SUBREDDIT_NAME, dt.year, dt.month, dt.day, POSTS_PER_DAY)
        assert record in output

def test_generate_reddit_comment(init_comments):
    assert len(db_session.query(Comment).all()) == DAYS * POSTS_PER_DAY * COMMENTS_PER_POST

    report = importlib.reload(utils.email_db_report)
    output = {tuple(item) for item in report.generate_reddit_comment(END_DT, DAYS, html=False)}
    assert len(output) == DAYS
    
    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        record = (SUBREDDIT_NAME, dt.year, dt.month, dt.day, COMMENTS_PER_POST * POSTS_PER_DAY)
        assert record in output

def test_generate_reddit_user(init_users):
    assert len(db_session.query(User).all()) == DAYS * USERS_PER_DAY

    report = importlib.reload(utils.email_db_report)
    output = {tuple(item)[1:] for item in report.generate_reddit_user(END_DT, DAYS, html=False)}
    assert len(output) == DAYS
    
    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        record = (dt.year, dt.month, dt.day, USERS_PER_DAY)
        assert record in output

def test_generate_reddit_mod_action(init_mod_actions):
    assert len(db_session.query(ModAction).all()) == DAYS * MOD_ACTIONS_PER_DAY

    report = importlib.reload(utils.email_db_report)
    output = {tuple(item) for item in report.generate_reddit_mod_action(END_DT, DAYS, html=False)}
    assert len(output) == DAYS

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        record = (SUBREDDIT_NAME, dt.year, dt.month, dt.day, MOD_ACTIONS_PER_DAY)
        assert record in output

def test_generate_experiment_new(init_experiments):
    assert len(db_session.query(Experiment).all()) == DAYS * EXPERIMENTS_PER_DAY

    report = importlib.reload(utils.email_db_report)
    output = {tuple(item)[1:] for item in report.generate_experiment_new(END_DT, DAYS, html=False)}
    assert len(output) == DAYS

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        record = (dt.year, dt.month, dt.day, EXPERIMENTS_PER_DAY)
        assert record in output

def test_generate_experiment_active(init_experiments):
    assert len(db_session.query(Experiment).all()) == DAYS * EXPERIMENTS_PER_DAY

    report = importlib.reload(utils.email_db_report)
    output = list(report.generate_experiment_active(END_DT, DAYS, html=False)['total count'].values())
    assert len(output) == DAYS

    for day in range(DAYS):
        assert output[day] == EXPERIMENTS_PER_DAY

def test_generate_experiment_thing(init_experiment_things):
    count = DAYS * EXPERIMENTS_PER_DAY * THINGS_PER_EXPERIMENT
    assert len(db_session.query(ExperimentThing).all()) == count

    report = importlib.reload(utils.email_db_report)
    output = set(report.generate_experiment_thing(END_DT, DAYS, html=False))
    assert len(output) == DAYS * EXPERIMENTS_PER_DAY

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        for experiment in range(EXPERIMENTS_PER_DAY):
            experiment_id, _ = get_experiment_test_info(day, experiment)
            label = "(%s, %s)" % (experiment_id, ThingType.SUBMISSION.name)
            record = (label, dt.year, dt.month, dt.day, THINGS_PER_EXPERIMENT)
            assert record in output

def test_generate_experiment_thing_snapshot(init_experiment_thing_snapshots):
    count = DAYS * EXPERIMENTS_PER_DAY * THINGS_PER_EXPERIMENT * SNAPSHOTS_PER_THING
    assert len(db_session.query(ExperimentThingSnapshot).all()) == count

    report = importlib.reload(utils.email_db_report)
    output = set(report.generate_experiment_thing_snapshot(END_DT, DAYS, html=False))
    assert len(output) == DAYS * EXPERIMENTS_PER_DAY

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        for experiment in range(EXPERIMENTS_PER_DAY):
            experiment_id, _ = get_experiment_test_info(day, experiment)
            label = "(%s, %s)" % (experiment_id, ThingType.SUBMISSION.name)
            record = (label, dt.year, dt.month, dt.day, SNAPSHOTS_PER_THING * THINGS_PER_EXPERIMENT)
            assert record in output

def test_generate_experiment_action(init_experiment_actions):
    count = DAYS * EXPERIMENTS_PER_DAY * ACTIONS_PER_EXPERIMENT
    assert len(db_session.query(ExperimentAction).all()) == count

    report = importlib.reload(utils.email_db_report)
    output = set(report.generate_experiment_action(END_DT, DAYS, html=False))
    assert len(output) == DAYS * EXPERIMENTS_PER_DAY

    for day in range(DAYS):
        dt = END_DT - timedelta(days=day)
        for experiment in range(EXPERIMENTS_PER_DAY):
            experiment_id, _ = get_experiment_test_info(day, experiment)
            label = "(%s, %s)" % (experiment_id, "test")
            record = (label, dt.year, dt.month, dt.day, ACTIONS_PER_EXPERIMENT)
            assert record in output

