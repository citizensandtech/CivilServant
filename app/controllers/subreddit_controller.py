import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
import sqlalchemy
import app.event_handler
from utils.common import PageType, thing2dict
from app.models import Base, SubredditPage, Subreddit, Post, User

class SubredditPageController:
    def __init__(self, subname, db_session, r, log):
        self.subname = subname
        self.db_session = db_session
        self.log = log
        self.r = r
        self.fetched_posts = []
        self.fetched_subreddit_id = None
  
    def fetch_subreddit_page(self, pg_type, limit=300, return_praw_object=False):
        sub = self.r.subreddit(self.subname)
        self.fetched_subreddit_id = sub.id

        # fetch subreddit posts from reddit
        try:
            if pg_type==PageType.TOP:
                self.fetched_posts = sub.top(limit=limit)
            elif pg_type==PageType.CONTR:
                self.fetched_posts = sub.controversial(limit=limit)
            elif pg_type==PageType.NEW:
                self.fetched_posts = sub.new(limit=limit)   
            elif pg_type==PageType.HOT:
                self.fetched_posts = sub.hot(limit=limit)   
        except:
            self.log.error("Error querying /r/{0} {1} page: {2}".format(self.subname, pg_type.name, str(e)))
            return []         
        self.fetched_posts = list(self.fetched_posts)
        self.log.info("Queried /r/{0} {1} page".format(self.subname, pg_type.name))
        # add sub to subreddit table if not already there
        try:
            if self.archive_subreddit(sub):
                self.log.info("Saved new record for subreddit /r/{0}".format(self.subname))
        except:
            self.log.error("Failed to save new record for subreddit /r/{0}".format(self.subname))

        # save subreddit posts to database
        json_posts = []
        try:
            for post in self.fetched_posts:
                #new_post = post.json_dict #if("json_dict" in dir(post)) else post['data'] ### TO HANDLE TEST FIXTURES
                new_post = thing2dict(post)
                pruned_post = {
                    'id': new_post['id'],
                    'author': new_post['author'],
                    'num_comments': new_post['num_comments'],
                    'subreddit_id': new_post['subreddit_id'],
                    'score': new_post['score'],
                    'num_reports': new_post['num_reports'],
                    'user_reports': len(new_post['user_reports']),
                    'mod_reports': len(new_post['mod_reports']),
                    'created_utc':new_post['created_utc']
                }
                json_posts.append(pruned_post)
                #is_new_post = self.archive_post(post.json_dict)
                is_new_post = self.archive_post(new_post)
                is_new_user = self.archive_user(pruned_post['author']['name'], datetime.datetime.fromtimestamp(post.created))
            self.log.info("Saved posts from /r/{0} {1} page.".format(self.subname, pg_type.name))
        except sqlalchemy.exc.IntegrityError as e:
            self.log.info("Error Saving posts from /r/{0} {1} page: {2}".format(self.subname, pg_type.name, str(e)))
        except Exception as e:
            self.log.error("Error Saving posts from /r/{0} {1} page: {2}".format(self.subname, pg_type.name, str(e)))
        
        if(return_praw_object):
            return self.fetched_posts
        else:
            return json_posts

    @app.event_handler.event_handler
    def archive_subreddit_page(self, pg_type=PageType.HOT):
        posts = self.fetch_subreddit_page(pg_type, return_praw_object=False)

        self.db_session.insert_retryable(SubredditPage, {
            "page_type": pg_type.value,
            "subreddit_id": posts[0]['subreddit_id'].replace("t5_",""),
            "page_data": json.dumps(posts),
            "is_utc": True})

        #posts = self.fetch_subreddit_page(pg_type, return_praw_object=False)
        #subreddit_page = SubredditPage(created_at = datetime.datetime.utcnow(),
        #                        page_type = pg_type.value, 
        #                        subreddit_id = posts[0]['subreddit_id'].replace("t5_",""),
        #                        page_data = json.dumps(posts),
        #                        is_utc = True
        #                        )
        #self.db_session.add(subreddit_page)
        #self.db_session.commit()


    """ 
        returns True if it archives a new subreddit. 
        returns False if the subreddit does not need to be archived.
    """
    def archive_subreddit(self, sub):
        queried_sub = self.db_session.query(Subreddit).filter(Subreddit.id == sub.id).first()

        # if sub not in table, add it
        if not queried_sub:
            self.db_session.insert_retryable(Subreddit, {
                "id": sub.id,
                "name": sub.display_name})
            #new_sub = Subreddit(id = sub.id, 
            #                    name = sub.display_name)
            #self.db_session.add(new_sub)
            #self.db_session.commit()
            return True

        # else don't add it to subreddit table
        return False


    """ 
        note that 'post' is of type dictionary (has already been initially processed in fetch_subreddit_page)

        returns True if it archives a new post. 
        returns False if the post does not need to be archived.
    """
    def archive_post(self, post_info):
        queried_post = self.db_session.query(Post).filter(Post.id == post_info['id']).first()

        # if sub not in table, add it
        if not queried_post:
            self.db_session.insert_retryable(Post, {
                "id": post_info['id'],
                "subreddit_id": post_info['subreddit_id'].strip("t5_"),
                "created": datetime.datetime.fromtimestamp(post_info['created_utc']),
                "post_data": json.dumps(post_info)})
            #new_post = Post(
            #        id = post_info['id'],
            #        subreddit_id = post_info['subreddit_id'].strip("t5_"), # janky
            #        created = datetime.datetime.fromtimestamp(post_info['created_utc']),        
            #        post_data = json.dumps(post_info))
            #self.db_session.add(new_post)
            #self.db_session.commit()
            return True

        # else don't add it to post table
        return False


    """ 
        seen_at is of type timestamp
        (to save on api calls, do not query reddit for user info!)

        returns True if it archives a new redditor. 
        returns False if the redditor was already in table.
    """
    def archive_user(self, username, seen_at):
        user = self.db_session.query(User).filter(User.name == username).first()

        if not user:
            #new_user = User(
            #        name = username,
            #        id = None,
            #        created = None,
            #        first_seen = seen_at,
            #        last_seen = seen_at, 
            #        user_data = None)
            #self.db_session.add(new_user)
            #self.db_session.commit()
            self.db_session.insert_retryable(User, {
                "name": username,
                "id": None,
                "created": None,
                "first_seen": seen_at,
                "last_seen": seen_at, 
                "user_data": None})
            return True
        else:
            if seen_at > user.last_seen:
                user.last_seen = seen_at
                self.db_session.commit()
            return False
