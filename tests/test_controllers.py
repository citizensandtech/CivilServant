import pytest
import os
from mock import Mock, patch
#import simplejson as json
import json
import praw
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob, datetime
import app.controllers.front_page_controller
import app.controllers.subreddit_controller
import app.controllers.comment_controller
import app.controllers.moderator_controller
from utils.common import PageType, DbEngine, json2obj

### LOAD THE CLASSES TO TEST
from app.models import Base, FrontPage, SubredditPage, Subreddit, Post 
from app.models import ModAction, Comment, User, EventHook
import app.cs_logger

## SET UP THE DATABASE ENGINE
## TODO: IN FUTURE, SET UP A TEST-WIDE DB SESSION
TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR  = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] ="test"

db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()

def clear_all_tables():
    db_session.query(EventHook).delete()
    db_session.query(FrontPage).delete()
    db_session.query(SubredditPage).delete()
    db_session.query(Subreddit).delete()
    db_session.query(Post).delete()
    db_session.query(User).delete()  
    db_session.query(ModAction).delete()    
    db_session.query(Comment).delete()      
    db_session.commit()    

def setup_function(function):
    clear_all_tables()

def teardown_function(function):
    clear_all_tables()

@patch('praw.Reddit', autospec=True)
@patch('praw.models.Subreddit', autospec=True)    
def test_archive_reddit_front_page(mock_subreddit, mock_reddit):
    ### TEST THE MOCK SETUP AND MAKE SURE IT WORKS
    ## TODO: I should not be mocking SQLAlchemy
    ## I should just be mocking the reddit API

    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

    with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
        sub_data = [child['data'] for child in json.loads(f.read())['data']['children']]
    mock_subreddit.top.return_value = sub_data
    mock_subreddit.controversial.return_value = sub_data
    mock_subreddit.new.return_value = sub_data
    mock_subreddit.hot.return_value = sub_data  
    patch('praw.')

    r.subreddit = Mock(praw.models.helpers.SubredditHelper)
    r.subreddit.return_value = mock_subreddit   

    
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

"""
  basic test for method archive_subreddit_page to insert timestamped pages to subreddit_pages table.
  analogous to test_archive_reddit_front_page.
"""
@patch('praw.Reddit', autospec=True)
@patch('praw.models.Subreddit', autospec=True)    
def test_archive_subreddit_page(mock_subreddit, mock_reddit):
    ### TODO: TEST THE MOCK SETUP WITH AN ACTUAL QUERY

    test_subreddit_name = "science"
    test_subreddit_id = "mouw"

    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

    # with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
    #     sub_data = json.loads(f.read())['data']['children']

    sub_data = []
    with open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=TEST_DIR)) as f:
        fixture = [x['data'] for x in json.loads(f.read())['data']['children']]
        for post in fixture:
            json_dump = json.dumps(post)
            postobj = json2obj(json_dump)
            sub_data.append(postobj)

    mock_subreddit.top.return_value = sub_data
    mock_subreddit.controversial.return_value = sub_data
    mock_subreddit.new.return_value = sub_data
    mock_subreddit.hot.return_value = sub_data  
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
@patch('praw.models.Subreddit', autospec=True)    
def test_archive_subreddit(mock_subreddit, mock_reddit):
    test_subreddit_name = "science"
    test_subreddit_id = "mouw"

    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

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
    log = app.cs_logger.get_logger(ENV, BASE_DIR)
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
@patch('praw.models.Submission', autospec=True)    
def test_fetch_post_comments(mock_submission, mock_reddit):
    with open("{script_dir}/fixture_data/post2.json".format(script_dir=TEST_DIR)) as f:
        post = json.loads(f.read())
    with open("{script_dir}/fixture_data/post2_comments.json".format(script_dir=TEST_DIR)) as f:
        post_comments = json.loads(f.read())
    
    r = mock_reddit.return_value
    mock_submission.comments = post_comments
    mock_submission.num_comments = len(post_comments)
    r.get_submission.return_value = mock_submission
    log = app.cs_logger.get_logger(ENV, BASE_DIR)
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
@patch('praw.models.Submission', autospec=True)    
def test_archive_all_missing_subreddit_post_comments(mock_submission, mock_reddit):

    ## SET UP MOCKS 
    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

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
def test_archive_mod_action_page(mock_reddit):
    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

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
    last_action_id, unique_action_count_1 = mac.archive_mod_action_page()
    db_session.commit()
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0])
    assert unique_action_count_1 == len(mod_action_fixtures[0])
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
    _, unique_action_count_after_dupes = mac.archive_mod_action_page()
    db_session.commit()
    db_count_after_dupes = db_session.query(ModAction).count()
    assert db_count_after_dupes == len(mod_action_fixtures[0])
    assert db_count_after_dupes + unique_action_count_after_dupes == len(mod_action_fixtures[0])

    # NOW ADD A NEW PAGE
    r.get_mod_log.return_value = mod_action_fixtures[1]
    patch('praw.')
    last_action_id, unique_action_count_2 = mac.archive_mod_action_page(after_id = mod_action_fixtures[0][-1]['id'])
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0]) + len(mod_action_fixtures[1])
    assert unique_action_count_1 + unique_action_count_2 == len(mod_action_fixtures[0]) + len(mod_action_fixtures[1])
    assert last_action_id == mod_action_fixtures[1][-1]['id']

@patch('praw.Reddit', autospec=True)
@patch('praw.models.Submission', autospec=True)    
def test_fetch_post_comments(mock_submission, mock_reddit):
    with open("{script_dir}/fixture_data/post2.json".format(script_dir=TEST_DIR)) as f:
        post = json.loads(f.read())
    with open("{script_dir}/fixture_data/post2_comments.json".format(script_dir=TEST_DIR)) as f:
        post_comments = json.loads(f.read())
    
    r = mock_reddit.return_value
    mock_submission.comments = post_comments
    mock_submission.num_comments = len(post_comments)
    r.get_submission.return_value = mock_submission
    log = app.cs_logger.get_logger(ENV, BASE_DIR)
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
@patch('praw.models.Submission', autospec=True)    
def test_archive_all_missing_subreddit_post_comments(mock_submission, mock_reddit):

    ## SET UP MOCKS 
    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

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
  log = app.cs_logger.get_logger(ENV, BASE_DIR)
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

@patch('praw.Reddit', autospec=True)
def test_archive_last_thousand_comments(mock_reddit):
    r = mock_reddit.return_value
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

    
    subreddit_name = "science"
    subreddit_id = "mouw"

    comment_fixtures = []
    for filename in sorted(glob.glob("{script_dir}/fixture_data/comments*".format(script_dir=TEST_DIR))):
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
    assert db_session.query(Comment).count() == len(comment_fixtures[0])
    assert cc.last_subreddit_id == subreddit_id
    assert len(cc.last_queried_comments) == len(comment_fixtures[0])

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
    log = app.cs_logger.get_logger(ENV, BASE_DIR)

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
    last_action_id, unique_action_count_1 = mac.archive_mod_action_page()
    db_session.commit()
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0])
    assert unique_action_count_1 == len(mod_action_fixtures[0])
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
    _, unique_action_count_after_dupes = mac.archive_mod_action_page()
    db_session.commit()
    db_count_after_dupes = db_session.query(ModAction).count()
    assert db_count_after_dupes == len(mod_action_fixtures[0])
    assert db_count_after_dupes + unique_action_count_after_dupes == len(mod_action_fixtures[0])

    # NOW ADD A NEW PAGE
    r.get_mod_log.return_value = mod_action_fixtures[1]
    patch('praw.')
    last_action_id, unique_action_count_2 = mac.archive_mod_action_page(after_id = mod_action_fixtures[0][-1]['id'])
    assert db_session.query(ModAction).count() == len(mod_action_fixtures[0]) + len(mod_action_fixtures[1])
    assert unique_action_count_1 + unique_action_count_2 == len(mod_action_fixtures[0]) + len(mod_action_fixtures[1])
    assert last_action_id == mod_action_fixtures[1][-1]['id']
