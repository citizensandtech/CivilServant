import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from utils.common import PageType
from app.models import Base, SubredditPage, Subreddit, Post, Comment
from sqlalchemy import and_
from sqlalchemy import text
import sqlalchemy

class CommentController:
    def __init__(self, db_session, r, log):
        self.db_session = db_session
        self.log = log
        self.r = r 

    def archive_missing_post_comments(self, post_id):
        post = self.db_session.query(Post).filter(Post.id == post_id).first()
        query_time = datetime.datetime.utcnow()
        submission = self.r.submission(id=post_id)
        self.log.info("Querying Missing Comments for {post_id}. Total comments to archive: {num_comments}".format(
            post_id = post_id,
            num_comments = submission.num_comments
        ))
        submission.replace_more_comments(limit=None, threshold=0)
        comments = []
        
        if(os.environ['CS_ENV'] =='test'):
            flattened_comments = submission.comments # already a JSON dict
        else:
            flattened_comments = [x.json_dict for x in praw.helpers.flatten_tree(submission.comments)]

        for comment in flattened_comments:
            if 'replies' in comment.keys():
                del comment['replies']
            comments.append(comment)

        post.comment_data = json.dumps(comments)
        post.comments_queried_at = query_time
        self.db_session.commit()
        self.log.info("Saved Missing Comments for {post_id}. Total comments: {num_comments}".format(
            post_id = post_id,
            num_comments = len(comments)
        ))


    ## NOTE: THIS METHOD CAN REQUIRE A VERY LARGE NUMBER OF
    ## REDDIT API CALLS, WITH 1 CALL PER 20 COMMENTS
    def archive_all_missing_subreddit_post_comments(self, subreddit_id):
        subreddit_id = subreddit_id.replace("t5_", "")
        posts_without_comments = self.db_session.query(Post).filter(
                and_(
                    Post.comment_data == None,
                    Post.subreddit_id == subreddit_id
                )).all()
        self.log.info("Archiving {count} posts from subreddit: {subreddit}".format(
            count = len(posts_without_comments),
            subreddit = subreddit_id
        ))
        for post in posts_without_comments:
            self.archive_missing_post_comments(post.id)

    def archive_last_thousand_comments(self, subreddit_name):
        # fetch the subreddit ID
        subreddit = self.db_session.query(Subreddit).filter(Subreddit.name == subreddit_name).first()

        # fetch the last thousand comment IDs
        comment_ids = [x['id'] for x in self.db_session.execute(text("select id from comments WHERE subreddit_id='{0}' ORDER BY created_utc DESC LIMIT 1000;".format(subreddit.id)))]

        # fetch comments from reddit
        comments = []
        try:
            limit_found = False
            after_id = None
            while(limit_found == False):
                comment_result = self.r.get_comments(subreddit = subreddit_name, params={"after":after_id}, limit=100)
                comments_returned = 0
                for comment in comment_result:
                    comments_returned += 1
                    if(os.environ['CS_ENV'] !='test'):
                        comment = comment.json_dict
                    if(comment['id'] in comment_ids):
                        limit_found = True
                    else:
                        comments.append(comment)
                    after_id = "t1_" + comment['id']
                if(comment_result is None or comments_returned == 0 ):
                    limit_found = True
        except praw.errors.APIException:
            self.log.error("Error querying latest {subreddit_name} comments from reddit API. Immediate attention needed.".format(subreddit_name=subreddit_name))
            sys.exit(1)
            
        db_comments = []
        for comment in comments:
            if((comment['id'] in comment_ids) != True):
                db_comment = Comment(
                    id = comment['id'],
                    subreddit_id = subreddit.id,
                    created_utc = datetime.datetime.utcfromtimestamp(comment['created_utc']),
                    post_id = comment['link_id'].replace("t3_" ,""),
                    user_id = comment['author'],
                    comment_data = json.dumps(comment)
                )
                db_comments.append(db_comment)
        try:
            self.db_session.add_all(db_comments)
            self.db_session.commit()
        except sqlalchemy.exc.DBAPIError as e:
            self.log.error("Error saving {0} comments to database. Immediate attention needed. Error: {1}".format(len(db_comments)),str(e))
        self.log.info("Fetching up to the last thousand comments in {subreddit_name}. Total comments archived: {num_comments}".format(
            subreddit_name = subreddit.name,
            num_comments = len(db_comments)
        ))

    def archive_last_thousand_comments(self, subreddit_name):
        # fetch the subreddit ID
        subreddit = self.db_session.query(Subreddit).filter(Subreddit.name == subreddit_name).first()

        # fetch the last thousand comment IDs
        comment_ids = [x['id'] for x in self.db_session.execute(text("select id from comments WHERE subreddit_id='{0}' ORDER BY created_utc DESC LIMIT 1000;".format(subreddit.id)))]

        # fetch comments from reddit
        comments = []
        try:
            limit_found = False
            after_id = None
            while(limit_found == False):
                comment_result = self.r.get_comments(subreddit = subreddit_name, params={"after":after_id}, limit=100)
                comments_returned = 0
                for comment in comment_result:
                    comments_returned += 1
                    if(os.environ['CS_ENV'] !='test'):
                        comment = comment.json_dict
                    if(comment['id'] in comment_ids):
                        limit_found = True
                    else:
                        comments.append(comment)
                    after_id = "t1_" + comment['id']
                if(comment_result is None or comments_returned == 0 ):
                    limit_found = True
        except praw.errors.APIException:
            self.log.error("Error querying latest {subreddit_name} comments from reddit API. Immediate attention needed.".format(subreddit_name=subreddit_name))
            sys.exit(1)
            
        db_comments = []
        for comment in comments:
            if((comment['id'] in comment_ids) != True):
                db_comment = Comment(
                    id = comment['id'],
                    subreddit_id = subreddit.id,
                    created_utc = datetime.datetime.utcfromtimestamp(comment['created_utc']),
                    post_id = comment['link_id'].replace("t3_" ,""),
                    user_id = comment['author'],
                    comment_data = json.dumps(comment)
                )
                db_comments.append(db_comment)
        try:
            self.db_session.add_all(db_comments)
            self.db_session.commit()
        except sqlalchemy.exc.DBAPIError as e:
            self.log.error("Error saving {0} comments to database. Immediate attention needed. Error: {1}".format(len(db_comments)),str(e))
        self.log.info("Fetching up to the last thousand comments in {subreddit_name}. Total comments archived: {num_comments}".format(
            subreddit_name = subreddit.name,
            num_comments = len(db_comments)
        ))
