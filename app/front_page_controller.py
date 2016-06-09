import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries


from app.models import Base, FrontPage

def fetch_top_reddit_front_page(r, limit=100):
    top_posts = []
    for post in r.get_top(limit=limit):
        if("json_dict" in dir(post)):
            top_posts.append(post.json_dict)
        else:
            top_posts.append(post) ### TO HANDLE TEST FIXTURES
    return top_posts

def archive_reddit_front_page(r, db_session):
    top_posts = fetch_top_reddit_front_page(r)
    front_page = FrontPage(created_at = datetime.datetime.now(),
                           page_data = json.dumps(top_posts))
    db_session.add(front_page)
    db_session.commit()