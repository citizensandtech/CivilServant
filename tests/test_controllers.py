import pytest
import os
from mock import Mock, patch
#import simplejson as json
import json
import sqlalchemy
from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import sessionmaker
import glob, datetime
import app.controllers.front_page_controller
import app.controllers.subreddit_controller
import app.controllers.comment_controller
import app.controllers.moderator_controller
import app.controllers.lumen_controller
import app.controllers.twitter_controller
import app.connections.twitter_connect
from utils.common import PageType, DbEngine, json2obj, TwitterUserState
import requests
import twitter

import utils
from utils.common import CS_JobState

### LOAD THE CLASSES TO TEST
from app.models import *
import app.cs_logger

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] ="test"

db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
log = app.cs_logger.get_logger(ENV, BASE_DIR)

def clear_all_tables():
    db_session.query(EventHook).delete()
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()
    db_session.query(ModAction).delete()
    db_session.query(Comment).delete()
    db_session.query(LumenNotice).delete()
    db_session.query(LumenNoticeToTwitterUser).delete()
    db_session.query(TwitterUser).delete()
    db_session.query(TwitterUserSnapshot).delete()
    db_session.query(TwitterStatus).delete()
    db_session.commit()

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_archive_reddit_front_page(mock_subreddit, mock_reddit):
    ### TEST THE MOCK SETUP AND MAKE SURE IT WORKS
    ## TODO: I should not be mocking SQLAlchemy
    ## I should just be mocking the reddit API

    r = mock_reddit.return_value

    with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
        sub_data = json.loads(f.read())['data']['children']
    mock_subreddit.get_top.return_value = sub_data
    mock_subreddit.get_controversial.return_value = sub_data
    mock_subreddit.get_new.return_value = sub_data
    mock_subreddit.get_hot.return_value = sub_data
    patch('praw.')

    r.get_subreddit.return_value = mock_subreddit

    assert len(db_session.query(FrontPage).all()) == 0

    ## NOW START THE TEST for top, controversial, new
    fp = app.controllers.front_page_controller.FrontPageController(db_session, r, log)
    fp.archive_reddit_front_page(PageType.TOP)
    fp.archive_reddit_front_page(PageType.CONTR)
    fp.archive_reddit_front_page(PageType.NEW)
    fp.archive_reddit_front_page(PageType.HOT)

    all_pages = db_session.query(FrontPage).all()
    assert len(all_pages) == 4

    top_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.TOP.value)
    assert top_pages.count() == 1

    contr_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.CONTR.value)
    assert contr_pages.count() == 1

    new_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.NEW.value)
    assert new_pages.count() == 1

    new_pages = db_session.query(FrontPage).filter(FrontPage.page_type == PageType.HOT.value)
    assert new_pages.count() == 1


####  basic test for method archive_subreddit_page to insert timestamped pages to subreddit_pages table.
####  analogous to test_archive_reddit_front_page.
@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_archive_subreddit_page(mock_subreddit, mock_reddit):
    ### TODO: TEST THE MOCK SETUP WITH AN ACTUAL QUERY

    test_subreddit_name = "science"
    test_subreddit_id = "mouw"

    r = mock_reddit.return_value

    # with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    #     sub_data = json.loads(f.read())['data']['children']

    sub_data = []
    with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
        fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
        for post in fixture:
            json_dump = json.dumps(post)
            postobj = json2obj(json_dump)
            sub_data.append(postobj)

    mock_subreddit.get_top.return_value = sub_data
    mock_subreddit.get_controversial.return_value = sub_data
    mock_subreddit.get_new.return_value = sub_data
    mock_subreddit.get_hot.return_value = sub_data
    patch('praw.')

    mock_subreddit.display_name = test_subreddit_name
    mock_subreddit.id = test_subreddit_id
    r.get_subreddit.return_value = mock_subreddit

    assert len(db_session.query(SubredditPage).all()) == 0
    sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)

    ## NOW START THE TEST for top, controversial, new
    sp.archive_subreddit_page(PageType.TOP)
    sp.archive_subreddit_page(PageType.CONTR)
    sp.archive_subreddit_page(PageType.NEW)
    sp.archive_subreddit_page(PageType.HOT)

    all_pages = db_session.query(SubredditPage).all()
    assert len(all_pages) == 4

    top_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.TOP.value).count()
    assert top_pages_count == 1
    top_page = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.TOP.value).first()
    assert top_page.subreddit_id == "mouw"

    contr_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.CONTR.value).count()
    assert contr_pages_count == 1

    new_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.NEW.value).count()
    assert new_pages_count == 1

    hot_pages_count = db_session.query(SubredditPage).filter(SubredditPage.page_type == PageType.HOT.value).count()
    assert hot_pages_count == 1


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_archive_subreddit(mock_subreddit, mock_reddit):
    test_subreddit_name = "science"
    test_subreddit_id = "mouw"

    r = mock_reddit.return_value

    mock_subreddit.display_name = test_subreddit_name
    mock_subreddit.id = test_subreddit_id
    patch('praw.')

    assert len(db_session.query(Subreddit).all()) == 0
    sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)

    ## NOW START THE TEST
    # TODO: should you even be allowed to archive a different subreddit than the one sp was made for?
    sp.archive_subreddit(mock_subreddit)

    all_subs = db_session.query(Subreddit).all()
    assert len(all_subs) == 1

    ## trying to archive it again should do nothing (don't throw errors, don't edit db)
    sp.archive_subreddit(mock_subreddit)

    all_subs = db_session.query(Subreddit).all()
    assert len(all_subs) == 1

@patch('praw.Reddit', autospec=True)
def test_archive_post(mock_reddit):

    # dummy post just to pass the test.
    # TODO: carefully describe what the types of these 'archive' method args should be...
    post = {
        'id': 1,
        'subreddit_id': 't5_mouw',
        'created': 1467348033.0,
        'created_utc': 1467319233.0
    }

    r = mock_reddit.return_value
    test_subreddit_name = "science"
    patch('praw.')

    assert len(db_session.query(Post).all()) == 0
    sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)

    ## NOW START THE TEST
    sp.archive_post(post)

    all_posts = db_session.query(Post).all()
    assert len(all_posts) == 1

    dbpost = db_session.query(Post).filter(Post.id==post['id']).first()
    assert dbpost.created == datetime.datetime.fromtimestamp(post['created_utc'])

    ## trying to archive it again should do nothing (don't throw errors, don't edit db)
    sp.archive_post(post)

    all_posts = db_session.query(Post).all()
    assert len(all_posts) == 1

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
def test_fetch_post_comments(mock_submission, mock_reddit):
    with open("{script_dir}/fixture_data/post2.json".format(script_dir=TEST_DIR)) as f:
        post = json.loads(f.read())
    with open("{script_dir}/fixture_data/post2_comments.json".format(script_dir=TEST_DIR)) as f:
        post_comments = json.loads(f.read())

    r = mock_reddit.return_value
    mock_submission.comments = post_comments
    mock_submission.num_comments = len(post_comments)
    r.get_submission.return_value = mock_submission
    patch('praw.')

    ## ADD THE FIXTURE POST TO THE DATABASE
    assert len(db_session.query(Post).all()) == 0
    test_subreddit_name = "science"
    sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)
    sp.archive_post(post)
    all_posts = db_session.query(Post).all()
    assert len(all_posts) == 1

    db_session.commit()
    dbpost = db_session.query(Post).filter(Post.id == post['id']).first()
    assert dbpost.comment_data == None
    assert dbpost.comments_queried_at == None


    ## NOW TEST FETCHING COMMENTS
    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    cc.archive_missing_post_comments(post['id'])

    db_session.commit()
    dbpost = db_session.query(Post).filter(Post.id == post['id']).first()
    assert dbpost.comment_data != None
    assert dbpost.comments_queried_at != None


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
def test_archive_all_missing_subreddit_post_comments(mock_submission, mock_reddit):

    ## SET UP MOCKS
    r = mock_reddit.return_value

    ## TO START, LOAD POST FIXTURES
    post_fixture_names = ["post.json", "post2.json"]
    post_comment_fixture_names = ["post_comments.json", "post2_comments.json"]

    test_post_index = 0
    test_post_subreddit = None
    post_fixtures = []
    post_fixture_comments = []

    for i in range(len(post_fixture_names)):
        post_fixture_name = post_fixture_names[i]
        post_comment_fixture_name = post_comment_fixture_names[i]

        with open("{script_dir}/fixture_data/{file}".format(script_dir=TEST_DIR, file=post_fixture_name)) as f:
            post = json.loads(f.read())
            post_fixtures.append(post)

        if(i == test_post_index):
            with open("{script_dir}/fixture_data/{file}".format(script_dir=TEST_DIR, file=post_comment_fixture_name)) as f:
                post_comments = json.loads(f.read())
                mock_submission.comments = post_comments
                mock_submission.num_comments = len(post_comments)
            test_post_subreddit = post['subreddit_id']
            post_fixture_comments.append(post_comments)
        else:
            post_fixture_comments.append(None)

        sp = app.controllers.subreddit_controller.SubredditPageController(post['subreddit_id'], db_session, r, log)
        sp.archive_post(post)
    db_session.commit()

    r.get_submission.return_value = mock_submission
    patch('praw.')

    ## NOW RUN THE TEST
    db_session.commit()
    dbpost = db_session.query(Post).filter(Post.id == post_fixtures[test_post_index]['id']).first()
    assert dbpost != None
    assert dbpost.comment_data == None
    assert dbpost.comments_queried_at == None

    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    cc.archive_all_missing_subreddit_post_comments(test_post_subreddit)
    db_session.commit()
    # CHECK THAT THE COMMENTS WERE ADDED
    dbpost = db_session.query(Post).filter(Post.id == dbpost.id).first()
    assert dbpost.comment_data != None
    assert dbpost.comments_queried_at != None

    # CHECK THE ONE FROM A DIFFERENT SUBREDDIT
    dbpost = db_session.query(Post).filter(Post.id == post_fixtures[test_post_index+1]['id']).first()
    assert dbpost.comment_data == None
    assert dbpost.comments_queried_at == None

### this test doesn't pass right now. not related to twitter/lumen code
### this test may have been deleted at some point?
@patch('praw.Reddit', autospec=True)
def test_archive_last_thousand_comments(mock_reddit):
    r = mock_reddit.return_value


    subreddit_name = "science"
    subreddit_id = "mouw"

    comment_fixtures = []
    for filename in glob.glob("{script_dir}/fixture_data/comments*".format(script_dir=TEST_DIR)):
        f = open(filename, "r")
        comment_fixtures.append(json.loads(f.read()))
        f.close()



    m = Mock()
    m.side_effect = [comment_fixtures[0][0:100],
                     comment_fixtures[0][100:200],
                     comment_fixtures[0][200:300],
                     comment_fixtures[0][300:400],
                     comment_fixtures[0][400:500],
                     comment_fixtures[0][500:600],
                     comment_fixtures[0][600:700],
                     comment_fixtures[0][700:800],
                     comment_fixtures[0][800:900],
                     comment_fixtures[0][900:],
                     []]

    r.get_comments = m
    patch('praw.')

    ## add science subreddit
    db_session.add(Subreddit(
        id = subreddit_id,
        name = subreddit_name))
    db_session.commit()

    cc = app.controllers.comment_controller.CommentController(db_session, r, log)

    assert db_session.query(Comment).count() == 0
    cc.archive_last_thousand_comments(subreddit_name)
    assert db_session.query(Comment).count() == 1000

    db_comment = db_session.query(Comment).order_by(app.models.Comment.created_utc.asc()).first()
    assert db_comment.subreddit_id == subreddit_id
    assert db_comment.post_id == comment_fixtures[0][-1]['link_id'].replace("t3_","")
    assert db_comment.user_id == comment_fixtures[0][-1]['author']
    assert len(db_comment.comment_data) > 0

    ## NOW TEST THAT NO OVERLAPPING IDS ARE ADDED
    first_ids = [x['id'] for x in comment_fixtures[0]]
    second_ids = [x['id'] for x in comment_fixtures[1] if (x['id'] in first_ids)!=True]

    m = Mock()
    m.side_effect = [comment_fixtures[1][0:100],
                     comment_fixtures[1][100:200],
                     comment_fixtures[1][200:300],
                     comment_fixtures[1][300:400],
                     comment_fixtures[1][400:500],
                     comment_fixtures[1][500:600],
                     comment_fixtures[1][600:700],
                     comment_fixtures[1][700:800],
                     comment_fixtures[1][800:900],
                     comment_fixtures[1][900:],
                     []]
    r.get_comments = m
    patch('praw.')
    cc.archive_last_thousand_comments(subreddit_name)
    db_session.commit()
    assert db_session.query(Comment).count() == len(first_ids) + len(second_ids)

@patch('praw.Reddit', autospec=True)
def test_archive_mod_action_page(mock_reddit):
    r = mock_reddit.return_value

    ## TO START, LOAD MOD ACTION FIXTURES
    mod_action_fixtures = []
    for filename in glob.glob("{script_dir}/fixture_data/mod_action*".format(script_dir=TEST_DIR)):
        f = open(filename, "r")
        mod_action_fixtures.append(json.loads(f.read()))
        f.close()

    subreddit = mod_action_fixtures[0][0]['sr_id36']

    r.get_mod_log.return_value = mod_action_fixtures[0]
    patch('praw.')

    mac = app.controllers.moderator_controller.ModeratorController(
        subreddit=subreddit, db_session=db_session, r=r, log=log
    )

    assert db_session.query(ModAction).count() == 0
    last_action_id = mac.archive_mod_action_page()
    db_session.commit()
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0])
    assert last_action_id == mod_action_fixtures[0][-1]['id']

    # makes sure all the properties were assigned
    action = mod_action_fixtures[0][0]
    db_action = db_session.query(ModAction).filter(ModAction.id == action['id']).first()

    assert db_action.id == action['id']
    assert db_action.created_utc == datetime.datetime.fromtimestamp(action['created_utc'])
    assert db_action.subreddit_id == action['sr_id36']
    assert db_action.mod == action['mod']
    assert db_action.target_author == action['target_author']
    assert db_action.action == action['action']
    assert db_action.target_fullname == action['target_fullname']
    assert db_action.action_data != None
    assert len(db_action.action_data) > 0


    # NOW TRY TO ADD DUPLICATES
    # AND ASSERT THAT NO DUPLICATES WERE ADDED
    mac.archive_mod_action_page()
    db_session.commit()
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0])

    # NOW ADD A NEW PAGE
    r.get_mod_log.return_value = mod_action_fixtures[1]
    patch('praw.')
    last_action_id = mac.archive_mod_action_page(after_id = mod_action_fixtures[0][-1]['id'])
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0]) + len(mod_action_fixtures[1])
    assert last_action_id == mod_action_fixtures[1][-1]['id']

@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
def test_fetch_post_comments(mock_submission, mock_reddit):
    with open("{script_dir}/fixture_data/post2.json".format(script_dir=TEST_DIR)) as f:
        post = json.loads(f.read())
    with open("{script_dir}/fixture_data/post2_comments.json".format(script_dir=TEST_DIR)) as f:
        post_comments = json.loads(f.read())

    r = mock_reddit.return_value
    mock_submission.comments = post_comments
    mock_submission.num_comments = len(post_comments)
    r.get_submission.return_value = mock_submission
    patch('praw.')

    ## ADD THE FIXTURE POST TO THE DATABASE
    assert len(db_session.query(Post).all()) == 0
    test_subreddit_name = "science"
    sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)
    sp.archive_post(post)
    all_posts = db_session.query(Post).all()
    assert len(all_posts) == 1

    db_session.commit()
    dbpost = db_session.query(Post).filter(Post.id == post['id']).first()
    assert dbpost.comment_data == None
    assert dbpost.comments_queried_at == None

    ## NOW TEST FETCHING COMMENTS
    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    cc.archive_missing_post_comments(post['id'])

    db_session.commit()
    dbpost = db_session.query(Post).filter(Post.id == post['id']).first()
    assert dbpost.comment_data != None
    assert dbpost.comments_queried_at != None


@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Submission', autospec=True)
def test_archive_all_missing_subreddit_post_comments(mock_submission, mock_reddit):

    ## SET UP MOCKS
    r = mock_reddit.return_value

    ## TO START, LOAD POST FIXTURES
    post_fixture_names = ["post.json", "post2.json"]
    post_comment_fixture_names = ["post_comments.json", "post2_comments.json"]

    test_post_index = 0
    test_post_subreddit = None
    post_fixtures = []
    post_fixture_comments = []

    for i in range(len(post_fixture_names)):
        post_fixture_name = post_fixture_names[i]
        post_comment_fixture_name = post_comment_fixture_names[i]

        with open("{script_dir}/fixture_data/{file}".format(script_dir=TEST_DIR, file=post_fixture_name)) as f:
            post = json.loads(f.read())
            post_fixtures.append(post)

        if(i == test_post_index):
            with open("{script_dir}/fixture_data/{file}".format(script_dir=TEST_DIR, file=post_comment_fixture_name)) as f:
                post_comments = json.loads(f.read())
                mock_submission.comments = post_comments
                mock_submission.num_comments = len(post_comments)
            test_post_subreddit = post['subreddit_id']
            post_fixture_comments.append(post_comments)
        else:
            post_fixture_comments.append(None)

        sp = app.controllers.subreddit_controller.SubredditPageController(post['subreddit_id'], db_session, r, log)
        sp.archive_post(post)
    db_session.commit()

    r.get_submission.return_value = mock_submission
    patch('praw.')

    ## NOW RUN THE TEST
    db_session.commit()
    dbpost = db_session.query(Post).filter(Post.id == post_fixtures[test_post_index]['id']).first()
    assert dbpost != None
    assert dbpost.comment_data == None
    assert dbpost.comments_queried_at == None

    cc = app.controllers.comment_controller.CommentController(db_session, r, log)
    cc.archive_all_missing_subreddit_post_comments(test_post_subreddit)
    db_session.commit()
    # CHECK THAT THE COMMENTS WERE ADDED
    dbpost = db_session.query(Post).filter(Post.id == dbpost.id).first()
    assert dbpost.comment_data != None
    assert dbpost.comments_queried_at != None

    # CHECK THE ONE FROM A DIFFERENT SUBREDDIT
    dbpost = db_session.query(Post).filter(Post.id == post_fixtures[test_post_index+1]['id']).first()
    assert dbpost.comment_data == None
    assert dbpost.comments_queried_at == None

@patch('praw.Reddit', autospec=True)
def test_archive_user(mock_reddit):

  username = "merrymou"
  seen_at = datetime.datetime.utcnow()

  r = mock_reddit.return_value
  test_subreddit_name = "science"
  patch('praw.')

  assert len(db_session.query(User).all()) == 0
  sp = app.controllers.subreddit_controller.SubredditPageController(test_subreddit_name, db_session, r, log)

  ## NOW START THE TEST
  sp.archive_user(username, seen_at)

  all_users = db_session.query(User).all()
  assert len(all_users) == 1

  user = db_session.query(User).first()
  old_last_seen = user.last_seen

  ## trying to archive it again should update last_seen field
  sp.archive_user(username, seen_at)

  all_users = db_session.query(User).all()
  assert len(all_users) == 1
  user = db_session.query(User).first()
  new_last_seen = user.last_seen
  assert(old_last_seen <= new_last_seen)




##########################################
########### LUMEN/TWITTER TESTS
##########################################
## we are not testing CS_JobState stuff

@patch('app.connections.lumen_connect.LumenConnect', autospec=True)
def test_archive_lumen_notices(mock_LumenConnect):
    lc = mock_LumenConnect.return_value
    with open("{script_dir}/fixture_data/anon_lumen_notices_0.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        lc.get_notices_to_twitter.return_value = json.loads(data)

    patch('app.connections.lumen_connect.')

    assert len(db_session.query(LumenNotice).all()) == 0

    lumen = app.controllers.lumen_controller.LumenController(db_session, lc, log)

    topics = ["Copyright"]


    date = datetime.datetime(2017, 6, 1, 0, 0) # 0 notices
    lumen.archive_lumen_notices(topics, date)
    assert len(db_session.query(LumenNotice).all()) == 0

    date = datetime.datetime(2017, 4, 15, 0, 0) # 21 notices from 4/15/2017 onward
    lumen.archive_lumen_notices(topics, date)
    assert len(db_session.query(LumenNotice).all()) == 21

    date = datetime.datetime(2017, 4, 12, 0, 0) # all 50 notices from 4/12/2017 onward
    lumen.archive_lumen_notices(topics, date)
    assert len(db_session.query(LumenNotice).all()) == 50

    date = datetime.datetime(2017, 4, 1, 0, 0) # 50 notices, but all repeats
    lumen.archive_lumen_notices(topics, date)
    assert len(db_session.query(LumenNotice).all()) == 50 # should stay the same

    # untested behavior: paging through + storing multiple pages of results

def mocked_requests_get(url):
    class MockResponse:
        def __init__(self, url):
            self.url = url
            if "t.co" in url:
                self.url = "https://twitter.com/this_was_tco"

        def url(self):
            return self.url

    return MockResponse(url)

@patch('requests.get', side_effect=mocked_requests_get)
def test_helper_parse_url_for_username(mock_get):

    test_cases = [
        ("https://twitter.com/sooos243/status/852942353321140224", "sooos243"),
        #("https://t.co/cDdD0cNOFd", "this_was_tco"),   # we are currently not unshortening t.cos
        ("https://twitter.com/account/suspended", utils.common.ParseUsernameSuspendedUserFound), # matters less right now since we aren't unshortening t.cos
        ("any other domain or string actually", None)
    ]

    for (url, result) in test_cases:
        try:
            assert app.controllers.lumen_controller.helper_parse_url_for_username(url, log) == result
        except utils.common.ParseUsernameSuspendedUserFound:
            if result == utils.common.ParseUsernameSuspendedUserFound:
                assert True
            else:
                assert False


@patch('requests.get', autospec=True)
@patch('app.connections.lumen_connect.LumenConnect', autospec=True)
def test_parse_notices_archive_users(mock_LumenConnect, mock_get):
    lc = mock_LumenConnect.return_value
    patch('app.connections.lumen_connect.')

    assert len(db_session.query(LumenNoticeToTwitterUser).all()) == 0

    lumen = app.controllers.lumen_controller.LumenController(db_session, lc, log)

    with open("{script_dir}/fixture_data/anon_lumen_notices_0.json".format(script_dir=TEST_DIR)) as f:
        data = json.loads(f.read())
        notices = data["notices"][:30] # to make test faster, but get >100 users
        for notice in notices:
            notice_record = LumenNotice(
                id                  = notice["id"],
                sender              = notice["sender_name"],
                principal           = notice["principal_name"],
                recipient           = notice["recipient_name"],
                notice_data         = json.dumps(notice))
            db_session.add(notice_record)
        db_session.commit()

    lumen_notices = db_session.query(LumenNotice).all()
    lumen.parse_notices_archive_users(lumen_notices)
    all_notices = db_session.query(LumenNoticeToTwitterUser).all()
    assert len(all_notices) == 175 # 140 if not anon fixture data

    not_found_users = [nu for nu in all_notices if utils.common.NOT_FOUND_TWITTER_USER_STR in nu.twitter_username]
    # because we are currently not unshortening t.co, we will know at least the username for each twitter user we find
    assert len(not_found_users) == 0



# archive_new_users makes sure that new users get a TwitterUser and TwitterUserSnapshot stored for them
# currently, it is NOT responsible for updating existing TwitterUser objects
# (archive_old_users is responsible for that, e.g. in the case that a user goes from found to not found)
# however, it should make sure that it doesn't add duplicate entries for the same user
#
# TODO: currently this test does not test user list with len>90, so as to not call api.UsersLookup more than once, which is difficult to mock

#### TODO: add a test for the case where multiple lumen notices mention an account that is not retrieved from Twitter
#### look in how user_names_to_notice_user is handled in the method
@patch('twitter.error', autospec=True)
@patch('twitter.Api', autospec=True)
def test_archive_new_users(mock_twitter_api, mock_twitter_error):
    api = mock_twitter_api.return_value
    te = mock_twitter_error.return_value
    tc = app.connections.twitter_connect.TwitterConnect()

    with open("{script_dir}/fixture_data/anon_twitter_users.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        api.UsersLookup.return_value = json.loads(data)
    tc.api = api

    api.GetUser.side_effect = te.TwitterError([{'message': 'User not found.', 'code': 50}])

    patch('twitter.')
    patch('app.connections.twitter_connect.')


    assert len(db_session.query(TwitterUser).all()) == 0

    twitter = app.controllers.twitter_controller.TwitterController(db_session, tc, log)

    with open("{script_dir}/fixture_data/anon_twitter_username_list.json".format(script_dir=TEST_DIR)) as f:
        users = json.loads(f.read())
        for name in users:
            noticeuser_record = LumenNoticeToTwitterUser(twitter_username = name)
            db_session.add(noticeuser_record)
        db_session.commit()

    for j in range(2):
        # archive_new_users should be idempotent
        # (running this twice shouldn't matter, as in, should not create duplicate records)

        if j == 0:
            # (i, (resulting found users, resulting not found users))
            params_results = [(20, (15, 3)), (len(users), (80, 3))]
        elif j == 1:
            params_results = [(20, (80, 3)), (len(users), (80, 3))]

        prev_limit = 0
        for (i, result) in params_results:
            users_list = users[prev_limit:i]
            noticeusers_list = db_session.query(LumenNoticeToTwitterUser).filter(
                LumenNoticeToTwitterUser.twitter_username.in_(users_list)).all()
            assert len(users_list) == len(noticeusers_list)

            twitter.archive_new_users(noticeusers_list)
            prev_limit = i

            found_users = db_session.query(TwitterUser).filter(TwitterUser.user_state == TwitterUserState.FOUND.value).all()
            found_user_snapshots = db_session.query(TwitterUserSnapshot).filter(TwitterUserSnapshot.user_state == TwitterUserState.FOUND.value).all()
            assert len(found_users) == result[0]
            assert len(found_user_snapshots) == result[0]

            not_found_users = db_session.query(TwitterUser).filter(TwitterUser.user_state == TwitterUserState.NOT_FOUND.value).all()
            not_found_user_snapshots = db_session.query(TwitterUserSnapshot).filter(TwitterUserSnapshot.user_state == TwitterUserState.NOT_FOUND.value).all()
            assert len(not_found_users) == result[1]
            assert len(not_found_user_snapshots) == result[1]

        all_users = db_session.query(TwitterUser).all()
        assert len(all_users) == len(set(users))

        assert len(db_session.query(TwitterUser.screen_name, func.count(TwitterUser.id)).group_by(TwitterUser.screen_name).having(func.count(TwitterUser.id) != 1).all()) == 0


# TODO: currently this test does not test user list with len>90, so as to not call api.UsersLookup more than once, which is difficult to mock
@patch('twitter.error', autospec=True)
@patch('twitter.Api', autospec=True)
def test_archive_old_users(mock_twitter_api, mock_twitter_error):
    api = mock_twitter_api.return_value
    te = mock_twitter_error.return_value
    tc = app.connections.twitter_connect.TwitterConnect()


    # for is_user_suspended_or_deleted
    api.GetUser.side_effect = te.TwitterError([{'message': 'User not found.', 'code': 50}])

    assert len(db_session.query(TwitterUser).all()) == 0
    assert len(db_session.query(TwitterUserSnapshot).all()) == 0


    now = datetime.datetime.utcnow()

    user_A_id = "888"
    user_B_id = "999"
    user_B_not_found_id = "<NOT_FOUND>_user_b_1498029635925.533"

    user_A_record = TwitterUser(
        id = user_A_id,
        not_found_id = None,
        screen_name = "user_a",
        created_at = now,
        record_created_at = now,
        lang = "en",
        user_state = TwitterUserState.FOUND.value,
        CS_oldest_tweets_archived = CS_JobState.NOT_PROCESSED.value)

    user_B_record = TwitterUser(
        id =  user_B_not_found_id,
        not_found_id = user_B_not_found_id,
        screen_name = "user_b",
        created_at = None,
        record_created_at = now,
        lang = None,
        user_state = TwitterUserState.NOT_FOUND.value,
        CS_oldest_tweets_archived = CS_JobState.PROCESSED.value) # no tweets to find

    db_session.add(user_A_record)
    db_session.add(user_B_record)
    db_session.commit()

    users = db_session.query(TwitterUser).all()
    key_to_user_A = {u.id: u for u in users if u.id == user_A_id}
    key_to_user_B =  {u.screen_name: u for u in users if u.id == user_B_not_found_id}

    ####################################################################

    # user A.1: has id before, find id again
    # expected behavior: add new snapshot
    with open("{script_dir}/fixture_data/anon_twitter_user_A.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        api.UsersLookup.return_value = json.loads(data)

    tc.api = api
    patch('twitter.')
    patch('app.connections.twitter_connect.')

    twitter = app.controllers.twitter_controller.TwitterController(db_session, tc, log)
    twitter.archive_old_users(key_to_user_A, has_ids=True)

    users = db_session.query(TwitterUser).filter(
        TwitterUser.id == user_A_id).all()
    assert len(users) == 1
    assert users[0].not_found_id == None

    snapshots = db_session.query(TwitterUserSnapshot).filter(
        TwitterUserSnapshot.twitter_user_id == user_A_id).all()
    assert len(snapshots) == 1
    assert snapshots[0].user_json is not None


    ####################################################################


    # user A.2: has id before, don't find id this time
    # expected behavior: add not_found_id to user object, add new snapshot
    api.UsersLookup.return_value = []
    tc.api = api
    patch('twitter.')
    patch('app.connections.twitter_connect.')

    twitter = app.controllers.twitter_controller.TwitterController(db_session, tc, log)
    twitter.archive_old_users(key_to_user_A, has_ids=True)

    users = db_session.query(TwitterUser).filter(
        TwitterUser.id == user_A_id).all()
    assert len(users) == 1
    assert users[0].not_found_id and utils.common.NOT_FOUND_TWITTER_USER_STR in users[0].not_found_id
    user_A_not_found_id = users[0].not_found_id

    snapshots = db_session.query(TwitterUserSnapshot).filter(
        or_(TwitterUserSnapshot.twitter_user_id == user_A_id,
            TwitterUserSnapshot.twitter_user_id == user_A_not_found_id)).all()
    assert len(snapshots) == 2
    for snapshot in snapshots:
        if snapshot.twitter_user_id is user_A_not_found_id:
            assert snapshot.user_json is None
        elif snapshot.twitter_user_id is user_A_id:
            assert snapshot.user_json is not None
            assert newest_snapshot.twitter_not_found_id and newest_snapshot.twitter_not_found_id == user_A_not_found_id

    ####################################################################

    # user B.1: has (only) screen_name before, don't find id this time
    # expected behavior: just add new snapshot
    api.UsersLookup.return_value = []
    tc.api = api
    patch('twitter.')
    patch('app.connections.twitter_connect.')

    twitter = app.controllers.twitter_controller.TwitterController(db_session, tc, log)
    twitter.archive_old_users(key_to_user_B, has_ids=False)

    users = db_session.query(TwitterUser).filter(
        or_(TwitterUser.id == user_B_not_found_id,
        TwitterUser.not_found_id == user_B_not_found_id)).all()
    assert len(users) == 1
    assert users[0].id == user_B_not_found_id
    assert users[0].not_found_id == user_B_not_found_id

    snapshots = db_session.query(TwitterUserSnapshot).filter(
            TwitterUserSnapshot.twitter_user_id == user_B_not_found_id).all()
    assert len(snapshots) == 1
    assert snapshots[0].twitter_not_found_id and snapshots[0].twitter_not_found_id == user_B_not_found_id


    ####################################################################


    # user B.2: has screen_name before, find id this time
    # expected behavior: (we assume screen_name stays the same between the time of query and time of response,
        # add user_id to TwitterUser object, add 1 new snapshot
    with open("{script_dir}/fixture_data/anon_twitter_user_B.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        api.UsersLookup.return_value = json.loads(data)

    tc.api = api
    patch('twitter.')
    patch('app.connections.twitter_connect.')

    twitter = app.controllers.twitter_controller.TwitterController(db_session, tc, log)

    # should work fine even if you know real id but call by screen_name
    for i in [1,2]:
        twitter.archive_old_users(key_to_user_B, has_ids=False)

        users = db_session.query(TwitterUser).filter(
            TwitterUser.not_found_id == user_B_not_found_id).all()
        assert len(users) == 1
        assert users[0].id == user_B_id

        snapshots = db_session.query(TwitterUserSnapshot).filter(
            TwitterUserSnapshot.twitter_user_id == user_B_id).all()
        assert len(snapshots) == i # add a new snapshot each time
        assert snapshots[0].twitter_not_found_id == user_B_not_found_id

        snapshots = db_session.query(TwitterUserSnapshot).filter(
            TwitterUserSnapshot.twitter_user_id == user_B_not_found_id).all()
        assert len(snapshots) == 1 # one from B1. B2 shouldn't have created a not_found_id snapshot

    ##########

    all_users = db_session.query(TwitterUser).all()
    assert len(all_users) == 2



# TODO: currently this test does not test users with lots of statuses/tweets, so as to not call api.GetUserTimeline more than once, which is difficult to mock
@patch('twitter.Api', autospec=True)
@patch('app.connections.twitter_connect.TwitterConnect', autospec=True)
def test_archive_user_tweets(mock_TwitterConnect, mock_twitter_api):
    tc = mock_TwitterConnect.return_value
    api = mock_twitter_api.return_value

    def mocked_GetUserTimeline(user_id, count=None, max_id=None):
        with open("{script_dir}/fixture_data/anon_twitter_tweets.json".format(script_dir=TEST_DIR)) as f:
            data = json.loads(f.read())
        assert len(data) == 200
        if user_id == "2" or user_id == "3": # suspended_user or protected_user
            raise twitter.error.TwitterError("Not authorized.") # not mocking TwitterError
        elif user_id == "1": # deleted_user
            raise twitter.error.TwitterError([{'message': 'Sorry, that page does not exist.', 'code': 34}])
        else:   # # existing_user ?
            return data


    m = Mock()
    m.side_effect = mocked_GetUserTimeline
    api.GetUserTimeline = m
    tc.api = api
    patch('twitter.')
    patch('app.connections.twitter_connect.')

    assert len(db_session.query(TwitterStatus).all()) == 0



    t_controller = app.controllers.twitter_controller.TwitterController(db_session, tc, log)

    user_results = [
        ({"screen_name": "existing_user", "id": "888", "user_state": TwitterUserState.FOUND.value}, {"status_count": 200, "user_state": TwitterUserState.FOUND.value}),
        ({"screen_name": "deleted_user", "id": "1", "user_state": TwitterUserState.NOT_FOUND.value}, {"status_count": 0, "user_state": TwitterUserState.NOT_FOUND.value}),
        ({"screen_name": "suspended_user", "id": "2", "user_state": TwitterUserState.NOT_FOUND.value}, {"status_count": 0, "user_state": TwitterUserState.SUSPENDED.value}),
        ({"screen_name": "protected_user", "id": "3", "user_state": TwitterUserState.PROTECTED.value}, {"status_count": 0, "user_state": TwitterUserState.PROTECTED.value})
    ]

    for i, (user, result) in enumerate(user_results):
        # need to create TwitterUser records first
        user_record = TwitterUser(
            id = user["id"],
            screen_name = user["screen_name"],
            user_state = user["user_state"])
        db_session.add(user_record)
        db_session.commit()

        t_controller.archive_user_tweets(user_record, backfill=True, is_test=True)
        user_record = db_session.query(TwitterUser).filter(TwitterUser.screen_name == user["screen_name"]).first()
        all_tweets = db_session.query(TwitterStatus).filter(TwitterStatus.user_id == user_record.id).all()
        assert len(all_tweets) == result["status_count"]
        assert user_record.user_state == result["user_state"]




"""
# TODO: currently this test does not test user list with len>90, so as to not call api.UsersLookup more than once, which is difficult to mock
@patch('twitter.error', autospec=True)
@patch('twitter.Api', autospec=True)
@patch('app.connections.twitter_connect.TwitterConnect', autospec=True)
def test_query_and_archive_user_snapshots_and_tweets(mock_TwitterConnect, mock_twitter_api, mock_twitter_error):
    tc = mock_TwitterConnect.return_value
    api = mock_twitter_api.return_value
    te = mock_twitter_error.return_value
    with open("{script_dir}/fixture_data/twitter_users.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        api.UsersLookup.return_value = json.loads(data)

    with open("{script_dir}/fixture_data/twitter_tweets.json".format(script_dir=TEST_DIR)) as f:
        data = f.read()
        api.GetUserTimeline.return_value = json.loads(data)

    tc.api = api
    patch('twitter.')
    patch('app.connections.twitter_connect.')



    tc.api = api

    api.GetUser.side_effect = te.TwitterError([{'message': 'User not found.', 'code': 50}])

    patch('twitter.')
    patch('app.connections.twitter_connect.')


    assert len(db_session.query(TwitterUser).all()) == 0

    twitter = app.controllers.twitter_controller.TwitterController(db_session, tc, log)

    with open("{script_dir}/fixture_data/twitter_username_list.json".format(script_dir=TEST_DIR)) as f:
        users = json.loads(f.read())

    twitter.archive_new_users(users)

    # same as asserts for archive_new_users
    found_users = db_session.query(TwitterUser).filter(TwitterUser.user_state == TwitterUserState.FOUND.value).all()
    found_user_snapshots = db_session.query(TwitterUserSnapshot).filter(TwitterUserSnapshot.user_state == TwitterUserState.FOUND.value).all()
    assert len(found_users) == 80
    assert len(found_user_snapshots) == 80

    not_found_users = db_session.query(TwitterUser).filter(TwitterUser.user_state == TwitterUserState.NOT_FOUND.value).all()
    not_found_user_snapshots = db_session.query(TwitterUserSnapshot).filter(TwitterUserSnapshot.user_state == TwitterUserState.NOT_FOUND.value).all()
    assert len(not_found_users) == 6
    assert len(not_found_user_snapshots) == 6

    all_notices = db_session.query(TwitterUser).all()
    assert len(all_notices) == len(set(users))

    assert len(db_session.query(TwitterUser.screen_name, func.count(TwitterUser.id)).group_by(TwitterUser.screen_name).having(func.count(TwitterUser.id) != 1).all()) == 0


    ##########################################
    # now run query_and_archive_user_snapshots_and_tweets
    future = datetime.datetime.utcnow() + datetime.timedelta(days=1)

    for i in range(2,4):
        twitter.query_and_archive_user_snapshots_and_tweets(future, is_test=True) # to archive all

        found_users = db_session.query(TwitterUser).filter(TwitterUser.user_state == TwitterUserState.FOUND.value).all()
        found_user_snapshots = db_session.query(TwitterUserSnapshot).filter(TwitterUserSnapshot.user_state == TwitterUserState.FOUND.value).all()
        assert len(found_users) == 80
        assert len(found_user_snapshots) == 80*i

        not_found_users = db_session.query(TwitterUser).filter(TwitterUser.user_state == TwitterUserState.NOT_FOUND.value).all()
        not_found_user_snapshots = db_session.query(TwitterUserSnapshot).filter(TwitterUserSnapshot.user_state == TwitterUserState.NOT_FOUND.value).all()
        assert len(not_found_users) == 6
        assert len(not_found_user_snapshots) == 6*i

        all_users = db_session.query(TwitterUser).all()
        assert len(all_users) == len(set(users))

        assert len(db_session.query(TwitterUser.screen_name, func.count(TwitterUser.id)).group_by(TwitterUser.screen_name).having(func.count(TwitterUser.id) != 1).all()) == 0

    ########################################

    # get tweets for 1 user (since we can't mock GetUserTimeline for multiple users...)
    not_found_user = None
    found_user = None
    for user in all_users:
        if utils.common.NOT_FOUND_TWITTER_USER_STR in user.id or user.user_state is TwitterUserState.PROTECTED:
            not_found_user = user
        else:
            found_user = user
        if not_found_user is not None and found_user is not None:
            break

    # can only have 1 found user, since we can't mock GetUserTimeline for so many users...
    twitter.with_user_records_archive_tweets([found_user, not_found_user], backfill=True, is_test=True)

    assert len(db_session.query(TwitterStatus).all()) == 200
    #assert [s.user_id for s in db_session.query(TwitterStatus).all()] == [found_user.id]
    assert len(db_session.query(TwitterStatus).filter(TwitterStatus.user_id == found_user.id).all()) == 200

    # with not_found_user, with_user_records_archive_tweets shouldn't call GetUserTimeline
    assert len(db_session.query(TwitterStatus).filter(TwitterStatus.user_id == not_found_user.id).all()) == 0
"""




"""
t.query_and_archive_user_snapshots_and_tweets(date)

Should also test:
    lumen.query_and_parse_notices_archive_users()
    t.query_and_archive_new_users()
    t.query_and_archive_tweets(username)
"""
