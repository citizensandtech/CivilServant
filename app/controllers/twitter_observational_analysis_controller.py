import inspect, os, sys, pytz
import simplejson as json
import datetime
import numpy as np
from app.models import Base, TwitterUser, TwitterStatus, LumenNotice, TwitterUserSnapshot, LumenNoticeToTwitterUser
from sqlalchemy import and_, or_, func

utc=pytz.UTC

def append_to_csv(fname, row):
    with open(fname, "a") as f:
        f.write(",".join(row) + "\n")

class TwitterObservationalAnalysisController:
    def __init__(self, output_dir, db_session, log):
        self.output_dir = output_dir
        self.db_session = db_session
        self.log = log

        self.basic_profiling_header = sorted(["user_id", "user_created_at_time", "num_tweets", "first_notice_time", "oldest_snapshot_status", "newest_snapshot_status", "oldest_tweet_time", "newest_tweet_time", \
                  "first_notice-created_at_time", "first_notice-oldest_tweet_time", "newest_tweet-first_notice_time"])
        self.basic_profiling_fname = "basic_profiling3.csv"

    def basic_profiling_create_csv(self):
        with open(self.basic_profiling_fname, "w") as f:
            f.write(",".join(self.basic_profiling_header) + "\n")


    def basic_profiling_create_dataset(self):
        self.basic_profiling_create_csv()
        #dataset = {}
        all_users = self.db_session.query(TwitterUser).all()

        for user in all_users:
            #data = dataset[user.id] 
            data = {key:None for key in self.basic_profiling_header}
            data["user_id"] = user.id
            data["user_created_at_time"] = user.created_at

            statuses_info = self.db_session.query(
                func.count(TwitterStatus.id), 
                func.min(TwitterStatus.created_at), 
                func.max(TwitterStatus.created_at)).filter(
                    or_(TwitterStatus.user_id == user.id,
                TwitterStatus.user_id == user.not_found_id)).all()

            data["num_tweets"] = statuses_info[0][0]
            data["oldest_tweet_time"] = statuses_info[0][1]
            data["newest_tweet_time"] = statuses_info[0][2]

            snapshots = self.db_session.query(
                TwitterUserSnapshot.user_state).filter(
                or_(TwitterUserSnapshot.twitter_user_id == user.id, 
                    TwitterUserSnapshot.twitter_user_id == user.not_found_id)).order_by(
                TwitterUserSnapshot.record_created_at).all()

            data["oldest_snapshot_status"] = snapshots[0][0]
            data["newest_snapshot_status"] = snapshots[-1][0]

            notice_ids = self.db_session.query(
                LumenNoticeToTwitterUser.notice_id).filter(
                or_(LumenNoticeToTwitterUser.twitter_user_id == user.id, 
                    LumenNoticeToTwitterUser.twitter_user_id == user.not_found_id, 
                    LumenNoticeToTwitterUser.twitter_username == user.screen_name)).all()

            notice_ids = [n[0] for n in notice_ids]

            notice_time = self.db_session.query(
                func.min(LumenNotice.record_created_at)).filter(
                LumenNotice.id.in_(notice_ids)).all()

            data["first_notice_time"] = notice_time[0][0]

            if data["first_notice_time"] is not None and data["user_created_at_time"] is not None:
                data["first_notice-created_at_time"] = (data["first_notice_time"] - data["user_created_at_time"]).days
            if data["first_notice_time"] is not None and data["oldest_tweet_time"] is not None:
                data["first_notice-oldest_tweet_time"] = (data["first_notice_time"] - data["oldest_tweet_time"]).days 
            if data["newest_tweet_time"] is not None and data["first_notice_time"] is not None:
                data["newest_tweet-first_notice_time"] = (data["newest_tweet_time"] - data["first_notice_time"]).days                       

            row = [str(data[k]) for k in self.basic_profiling_header]
            append_to_csv(self.basic_profiling_fname, row)

        """

            self.db_session.query(TwitterStatus).filter()

            statuses_info = self.db_session.query(
                func.count(TwitterStatus.id), func.min(TwitterStatus.created_at), func.max(TwitterStatus.created_at)).filter(
                TwitterStatus.user_id == user.id).all()

            self.log.info(statuses_info)
            #dataset[user.id]["num_tweets"] = statuses_info[0][0]
            #dataset[user.id]["oldest_tweet_time"] = statuses_info[0][1]
            #dataset[user.id]["newest_tweet_time"] = statuses_info[0][1]
        """
    
    """
    # calls create_datasets() and produces two different csv files, 
    # twitter_observational_analysis_{0}_{1}_{2}_posts.csv

    # timestamped, from these lists of dicts
    #
    # expecting id of subbreddit, e.g. "2qh13"
    # dates passed as strings "MM.YYYY"
    # dates passed as strings "MM.YYYY"
    # "/mnt/samba/reddit-archive/03.2017"
    # start_date = datetime.datetime.strptime(start_date, "%m.%Y")
    # end_date = datetime.datetime.strptime(end_date, "%m.%Y")    
    def create_csvs(self, frontpage_limit=10):
        self.log.info("Creating csvs for posts and comments from {0} to {1}".format(self.begin_date_utc, self.end_date_utc))
        start_date_str = "{0}.{1}".format(self.start_date_utc.month, self.start_date_utc.year)
        end_date_str = "{0}.{1}".format(self.end_date_utc.month, self.end_date_utc.year)
        
        posts_fname = "sticky_comment_power_analysis_{0}_{1}_{2}_posts.csv".format(self.subreddit.id, start_date_str, end_date_str)
        post_heading = ["id","created.utc","author","body.length","weekday","url","is.selftext","visible","num.comments","num.comments.removed","front_page","author.prev.posts","author.prev.participation"]
        with open(os.path.join(self.output_dir, posts_fname), "w") as f:
            f.write(",".join(post_heading) + "\n")

        comments_fname = "sticky_comment_power_analysis_{0}_{1}_{2}_comments.csv".format(self.subreddit.id, start_date_str, end_date_str)
        comment_heading = ["id","created.utc","author","body.length","toplevel","post.id","visible","post.visible","post.author","author.prev.comments","author.prev.participation"]
        with open(os.path.join(self.output_dir, comments_fname), "w") as f:
            f.write(",".join(comment_heading) + "\n")

        self.create_datasets(frontpage_limit)
        
        pruned_posts = {pid: self.posts[pid] for pid in self.posts if self.posts[pid]["created.utc"] >= self.start_date_utc}
        pruned_comments = {cid: self.comments[cid] for cid in self.comments if self.comments[cid]["created.utc"] >= self.start_date_utc}        
        with open(os.path.join(self.output_dir, posts_fname), "a") as f:
            for post_id in pruned_posts:
                row = [str(post_id)] + [str(self.posts[post_id][label]) for label in post_heading[1:]]
                f.write(",".join(row) + "\n")
        with open(os.path.join(self.output_dir, comments_fname), "a") as f:
            for comment_id in pruned_comments:
                row = [str(comment_id)] + [str(self.comments[comment_id][label]) for label in comment_heading[1:]]
                f.write(",".join(row) + "\n")

    def get_subreddit(self, subreddit_id):
        subreddit = self.db_session.query(Subreddit).filter(Subreddit.id == subreddit_id).first()
        return subreddit

    # Returns Two Lists of Dicts, Where Each Dict Contains One Row
    def create_datasets(self, frontpage_limit):
        # get posts, comments, modlog from (self.start_date_utc - 6 months) to self.end_date_utc

        self.log.info("Getting posts...")
        self.posts = self.get_posts()
        self.log.info("Getting comments...")
        self.comments = self.get_comments()

        self.log.info("Getting modlog...")
        (self.mod_actions_comments, self.mod_actions_posts) = self.get_modlog()
        self.post_to_comment_info = self.get_post_to_comment_info() # needs mod_actions
        self.log.info("Getting frontpages...")
        self.frontpages = self.get_frontpage_data()  


        # posts, comments = apply_post_flair(posts, comments)    # get post flair - don't do right now
        self.log.info("Applying modlog...")
        self.apply_mod_actions() # posts, comments   # get visible posts
        self.log.info("Applying frontpages...")
        self.apply_frontpage_data(frontpage_limit) # posts   # get front page minutes
        self.log.info("Applying participation and post to comment info...")
        self.apply_participation_and_post_to_comment_info() # posts, comments   # count prev posts
    """