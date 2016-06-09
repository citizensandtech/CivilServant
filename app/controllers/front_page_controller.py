import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from utils.common import PageType

from app.models import Base, FrontPage

def fetch_reddit_front_page(r, pg_type, limit=100):
    posts = []

    fetched = []
    
    if pg_type==PageType.TOP:
        fetched = r.get_top(limit=limit)
    elif pg_type==PageType.CONTR:
        fetched = r.get_controversial(limit=limit)
    
    for post in fetched:
        if("json_dict" in dir(post)):
            posts.append(post.json_dict)
        else:
            posts.append(post) ### TO HANDLE TEST FIXTURES
    return posts

def archive_reddit_front_page(r, db_session, pg_type=PageType.TOP):
    posts = fetch_reddit_front_page(r, pg_type)
    front_page = FrontPage(created_at = datetime.datetime.now(),
                           page_type = pg_type.value, 
                           page_data = json.dumps(posts))
    db_session.add(front_page)
    db_session.commit()