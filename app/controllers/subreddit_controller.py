import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from utils.common import PageType

from app.models import Base, SubredditPage, Subreddit, Post

def fetch_subreddit_page(r, db_session, subname, pg_type, limit=100):
    posts = []

    fetched = []
    
    sub = r.get_subreddit(subname)
    
    # add sub to subreddit table if not already there
    archive_subreddit(r, db_session, sub)

    if pg_type==PageType.TOP:
        fetched = sub.get_top(limit=limit)
    elif pg_type==PageType.CONTR:
        fetched = sub.get_controversial(limit=limit)
    elif pg_type==PageType.NEW:
        fetched = sub.get_new(limit=limit)    

    for post in fetched:
        new_post = post.json_dict if("json_dict" in dir(post)) else post ### TO HANDLE TEST FIXTURES
        posts.append(new_post)
        archive_post(r, db_session, post)
    return posts

def archive_subreddit_page(r, db_session, subname, pg_type=PageType.TOP):
    posts = fetch_subreddit_page(r, db_session, subname, pg_type)
    subreddit_page = SubredditPage(created_at = datetime.datetime.now(),
                           page_type = pg_type.value, 
                           page_data = json.dumps(posts))
    db_session.add(subreddit_page)
    db_session.commit()


def archive_subreddit(r, db_session, sub):
    sub_count = db_session.query(Subreddit).filter(Subreddit.id == sub.id).count()

    # if sub not in table, add it
    if sub_count == 0:
        new_sub = Subreddit(id = sub.id, 
                            name = sub.display_name)
        db_session.add(new_sub)
        db_session.commit()
    # else don't add it to subreddit table

def archive_post(r, db_session, post):
    post_count = db_session.query(Post).filter(Post.id == post.id).count()

    # if sub not in table, add it
    if post_count == 0:
        new_post = Post(
                id = post.id,
                subreddit_id = post.subreddit_id.strip("t5_"), # janky
                created = datetime.datetime.fromtimestamp(post.created),        
                post_data = json.dumps(post.json_dict if "json_dict" in dir(post) else post)) ### else is TO HANDLE TEST FIXTURES
        db_session.add(new_post)
        db_session.commit()
    # else don't add it to post table



