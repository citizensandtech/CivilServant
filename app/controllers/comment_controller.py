import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from utils.common import PageType
from app.models import Base, SubredditPage, Subreddit, Post
from sqlalchemy import and_

class CommentController:
    def __init__(self, db_session, r, log):
        self.db_session = db_session
        self.log = log
        self.r = r 

    def archive_missing_post_comments(self, post_id):
        post = self.db_session.query(Post).filter(Post.id == post_id).first()
        query_time = datetime.datetime.utcnow()
        submission = self.r.get_submission(submission_id=post_id)
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
            
        


