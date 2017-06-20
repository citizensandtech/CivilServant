import inspect, os, sys, pytz


### LOAD ENVIRONMENT VARIABLES
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
ENV = os.environ['CS_ENV']

sys.path.append(BASE_DIR)


import simplejson as json
import datetime
import numpy as np
from app.models import Base, TwitterUser, TwitterStatus, LumenNotice, TwitterUserSnapshot, LumenNoticeToTwitterUser
from sqlalchemy import and_, or_, func

utc=pytz.UTC
TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"


def append_rows_to_csv(dataframe, header, output_dir, fname):
    with open(os.path.join(output_dir, fname), "a") as f:
        for uid in dataframe:
            row = [str(dataframe[uid][label]) for label in header[1:]]
            f.write(",".join(row) + "\n")

def date_to_str(date):
    return "{0}-{1}-{2}".format(date.year, date.month, date.day)

class TwitterObservationalAnalysisController:
    def __init__(self, start_date, end_date, min_observed_days, output_dir, db_session, log):
        self.start_date_utc = utc.localize(start_date) if (start_date.tzinfo is None or start_date.tzinfo.utcoffset(d) is None) else start_date
        self.end_date_utc = utc.localize(end_date) if (end_date.tzinfo is None or end_date.tzinfo.utcoffset(d) is None) else end_date

        self.min_observed_days = min_observed_days
        now = utc.localize(datetime.datetime.utcnow())
        self.end_date_utc = min(
            self.end_date_utc, now - datetime.timedelta(days=self.min_observed_days))

        self.output_dir = output_dir
        self.db_session = db_session
        self.log = log

        # basic_profiling was used for initial data exploration
        self.basic_profiling_header = sorted(["user_id", "user_created_at_time", "num_tweets", "first_notice_time", "oldest_snapshot_status", "newest_snapshot_status", "oldest_tweet_time", "newest_tweet_time", \
                  "first_notice-created_at_time", "first_notice-oldest_tweet_time", "newest_tweet-first_notice_time"])
        self.basic_profiling_fname = "basic_profiling3.csv"



        self.user_ids = []

        self.tweet_day_header = ["id", "created_at", "user_language", "user_default_profile", \
            "user_verified", "date_first_notice_received", "notices_received", \
            "notices_received_on_day", "day_num", "before_first_notice", \
            "after_first_notice", "num_tweets", "num_media_tweets", "hours_unavailable", \
            "account_suspended", "account_deleted", "account_protected"]
        self.tweet_day_fname = "{0}_obs_analysis_tweet_day_{1}-{2}_n={3}.csv".format(
            date_to_str(now), 
            date_to_str(self.start_date_utc), date_to_str(self.end_date_utc),
            self.min_observed_days) 

        self.user_header = ["id", "created_at", "user_language", "user_default_profile", 
            "user_verified", "date_first_notice_received", "notices_received", "tweets_per_day_before_first_notice", 
            "tweets_per_day_after_first_notice", "hours_unavailable", "account_suspended", 
            "account_deleted", "account_protected", "notices_received"]
        self.user_fname = "{0}_obs_analysis_user_{1}-{2}_n={3}.csv".format(
            date_to_str(now), 
            date_to_str(self.start_date_utc), date_to_str(self.end_date_utc),
            self.min_observed_days) 

        self.twitter_users_to_notice_dates = None
        self.twitter_users_to_snapshots = None
        self.twitter_users_to_tweets = None
        self.twitter_users_day_nums = None
        self.default_day_num_dict_template = {
            "num_tweets": 0, 
            "num_media_tweets": 0,
            "num_notices": 0,
            "suspended": False, 
            "deleted": False,
            "protected": False
        }
        self.twitter_users_aggregates = None

        self.tweet_day_dataframe = {}
        self.user_dataframe = {}



    """
        calls create_datasets() and produces two different csv files,
            self.tweet_day_fname
            self.user_fname 
        timestamped, from these lists of dicts
            

        Range of date_received for lumen notices 
            from which to analyze users from 
            (needed because of fetch twitter statuses bug)
        Minimum Observed Time Since First Observation of Account, in days 
            (for example: 14 days would mean all accounts 
            whose notice was received by Lumen at least 14 days ago or more, 
            guaranteeing that we get at least 14 days for each observation). 
            Return only N days of observations before and after
    """
    def create_csvs(self):
        self.log.info("Creating csvs for tweet_days and users, from {0} to {1}; min_observed_days = {2}".format(
            date_to_str(self.start_date_utc), date_to_str(self.end_date_utc), self.min_observed_days))

        with open(os.path.join(self.output_dir, self.tweet_day_fname), "w") as f:
            f.write(",".join(self.tweet_day_header) + "\n")

        with open(os.path.join(self.output_dir, self.user_fname), "w") as f:
            f.write(",".join(self.user_header) + "\n")

        self.create_datasets()

        append_rows_to_csv(self.tweet_day_dataframe, self.tweet_day_header, self.output_dir, self.tweet_day_fname)
        append_rows_to_csv(self.user_dataframe, self.user_header, self.output_dir, self.user_fname)


    # Returns Two Lists of Dicts, Where Each Dict Contains One Row
    def create_datasets(self):

        # For all lumen notices received in [range of time] and were at least [x] days ago,
        self.log.info("Getting notice dates...")
        self.twitter_users_to_notice_dates = self.get_users_to_notice_dates()   # {user_id: [notice_date_received_day]}
        self.user_ids = self.twitter_users_to_notice_dates.keys()

        # Get user info
        # Get twitter users (use first snapshot for user info)
        self.log.info("Getting user snapshots...")
        self.twitter_users_to_snapshots = self.get_users_to_snapshots()

        #Get tweets
        #Need to do something about tweet deletion
        self.log.info("Getting user tweets...")        
        self.twitter_users_to_tweets = self.get_users_to_tweets()   # {user_id: [tweet_created_at_day]}

        #Calculate day_num relative to 1st notice day
        #Get user state per day, suspended/deleted/protected
        self.log.info("Getting user day nums...")        
        self.twitter_users_day_nums = self.get_users_day_nums()

        #Calculate aggregates
        self.log.info("Getting user aggregates...")        
        self.twitter_users_aggregates = self.get_aggregates()


        self.log.info("Creating dataframes...")        
        self.create_dataframes()

    def get_users_to_notice_dates(self):

        twitter_users_to_notice_dates = {} # {user_id: [notice_date_received_day]}
        notices = self.db_session.query(LumenNotice).filter(
            and_(LumenNotice.date_received >= self.start_date_utc,
                LumenNotice.date_received <= self.end_date_utc)).all()
        self.log.info("Retrieved {0} LumenNotice.".format(len(notices)))

        notice_id_to_date = {n.id: n.date_received for n in notices}
        notice_to_twitter_users = []
        if len(notice_id_to_date) > 0: 
            notice_to_twitter_users = self.db_session.query(LumenNoticeToTwitterUser).filter(
                LumenNoticeToTwitterUser.notice_id.in_(notice_id_to_date.keys())).all()
        self.log.info("Retrieved {0} LumenNoticeToTwitterUser.".format(len(notice_to_twitter_users)))        

        for ntu in notice_to_twitter_users:
            uid = ntu.twitter_user_id
            if uid not in twitter_users_to_notice_dates:
                twitter_users_to_notice_dates[uid] = []
            notice_date = notice_id_to_date[ntu.notice_id]
            twitter_users_to_notice_dates[uid].append(notice_date)

        self.log.info("Retrieved {0} TwitterUser.".format(len(twitter_users_to_notice_dates)))        
        return twitter_users_to_notice_dates

    def get_users_to_snapshots(self):
        twitter_users_to_snapshots = {} # {user_id: [snapshot]}
        num_snapshots = 0
        for uid in self.user_ids:
            snapshots = self.db_session.query(TwitterUserSnapshot).filter(
                or_(TwitterUserSnapshot.twitter_user_id == uid,
                    TwitterUserSnapshot.twitter_not_found_id == uid)).order_by(
                TwitterUserSnapshot.record_created_at).all()
            twitter_users_to_snapshots[uid] = snapshots
            num_snapshots += len(snapshots)
        self.log.info("Retrieved {0} TwitterUserSnapshot.".format(num_snapshots))
        return twitter_users_to_snapshots

    def get_users_to_tweets(self):
        twitter_users_to_tweets = {}    # {user_id: [tweet_created_at_day]}

        num_tweets = 0
        for uid in self.user_ids:
            tweets = self.db_session(TwitterStatus).filter(
                TwitterStatus.user_id == uid).order_by(
                TwitterUserSnapshot.created_at).all()
            twitter_users_to_tweets[uid] = tweets
            num_tweets += len(tweets)
        self.log.info("Retrieved {0} TwitterStatus.".format(num_tweets))
        return twitter_users_to_tweets

    def get_users_day_nums(self):
        #??? should we remove day_nums that are too large/small?
        twitter_users_day_nums = {}
        for uid in self.user_ids:
            twitter_users_day_nums[uid] = {}
            first_notice_date = self.twitter_users_to_notice_dates[uid][0]
            day_0 = datetime.datetime(first_notice_date.year, first_notice_date.month, first_notice_date.day)

            # num_notices
            for notice_date in self.twitter_users_to_notice_dates[uid]:
                day_num = (notice_date - day_0).days
                if day_num not in twitter_users_day_nums:
                    twitter_users_day_nums[day_num] = copy.deepcopy(self.default_day_num_dict_template)
                twitter_users_day_nums[day_num]["num_notices"] += 1

            # num_tweets, num_media_tweets 
            tweets = self.twitter_users_to_tweets[uid] if uid in self.twitter_users_to_tweets else []
            for tweet in tweets:
                day_num = (tweet.created_at - day_0).days
                if day_num not in twitter_users_day_nums:                 
                    twitter_users_day_nums[day_num] = copy.deepcopy(self.default_day_num_dict_template)
                twitter_users_day_nums[day_num]["num_tweets"] += 1

                # look for media entities
                # https://dev.twitter.com/overview/api/entities-in-twitter-objects
                status_data_json = json.loads(tweet.status_data)
                if "entities" in status_data_json and "media" in status_data_json["entities"]["media"]:
                    twitter_users_day_nums[day_num]["num_media_tweets"] += 1                    

            # suspended, deleted, protected
            for snapshot in self.twitter_users_to_snapshots:
                day_num = (snapshot.record_created_at - day_0).days
                if day_num not in twitter_users_day_nums:
                    twitter_users_day_nums[day_num] = copy.deepcopy(self.default_day_num_dict_template)
                if snapshot.user_state == TwitterUserState.NOT_FOUND:
                    twitter_users_day_nums[day_num]["deleted"] = True
                if snapshot.user_state == TwitterUserState.SUSPENDED:
                    twitter_users_day_nums[day_num]["suspended"] = True
                if snapshot.user_state == TwitterUserState.PROTECTED:
                    twitter_users_day_nums[day_num]["protected"] = True                  

        return twitter_users_day_nums

    def get_aggregates(self):
        #Calculate aggregates
        #prune ids. ???? should we do this?
        pruned_user_ids = []
        twitter_users_aggregates = {}
        for uid in self.user_ids:
            aggregates = {
                "total_unavailable_hours": 0,   # how to calculate this ????
                "num_days_before_day_0": 0,
                "num_days_after_day_0": 0,
                "ave_tweets_before_day_0": 0,
                "ave_tweets_after_day_0": 0,
                "account_suspended": False, # ever
                "account_deleted": False, # ever
                "account_protected": False, # ever           
            }

            aggregates["num_days_before_day_0"] = min(0, 0 - min(self.twitter_users_day_nums[uid]))
            aggregates["num_days_after_day_0"] = max(0, max(self.twitter_users_day_nums[uid]))
            if aggregates["num_days_before_day_0"] >= self.min_observed_days and aggregates["num_days_after_day_0"] >= self.min_observed_days:
                pruned_user_ids.append(uid)

            aggregates["total_unavailable_hours"] = 24*sum([1 for dn in self.twitter_users_day_nums[uid] if (dn["suspended"] or dn["deleted"] or dn["protected"])])
            aggregates["ave_tweets_before_day_0"] = sum(
                    [self.twitter_users_day_nums[uid][dn]["num_tweets"] 
                    for dn in self.twitter_users_day_nums[uid] if dn < 0]
                ) / aggregates["num_days_before_day_0"]
            aggregates["ave_tweets_after_day_0"] = sum(
                    [self.twitter_users_day_nums[uid][dn]["num_tweets"] 
                    for dn in self.twitter_users_day_nums[uid] if dn > 0]
                ) / aggregates["num_days_after_day_0"]

            aggregates["account_suspended"] = any([self.twitter_users_day_nums[uid][dn]["suspended"] for dn in self.twitter_users_day_nums[uid]])
            aggregates["account_deleted"] = any([self.twitter_users_day_nums[uid][dn]["deleted"] for dn in self.twitter_users_day_nums[uid]])
            aggregates["account_protected"] = any([self.twitter_users_day_nums[uid][dn]["protected"] for dn in self.twitter_users_day_nums[uid]])                        

            twitter_users_aggregates[user] = aggregates

        self.log.info("Pruned user_ids. {0} qualifying user_ids from {1} total.".format(len(pruned_user_ids), len(self.user_ids)))
        self.user_ids = pruned_user_ids

        return twitter_users_aggregates

    def create_dataframes(self):
        for uid in self.user_ids:
            user_json = json.loads(self.twitter_users_to_snapshots[uid][0].user_json)
            self.tweet_day_dataframe[uid] = {label: None for label in self.tweet_day_header}

            self.user_dataframe[uid] = {label: None for label in self.user_header}            
            self.user_dataframe[uid] = {
                "id": None,
                "created_at": None,
                "user_language": None,
                "user_default_profile": None,
                "user_verified": None,
                "date_first_notice_received": self.twitter_users_to_notice_dates[uid][0],
                "notices_received": len(self.twitter_users_to_notice_dates[uid]),
                "tweets_per_day_before_first_notice": self.twitter_users_aggregates[uid]["ave_tweets_before_day_0"], 
                "tweets_per_day_after_first_notice": self.twitter_users_aggregates[uid]["ave_tweets_before_day_0"], 
                "hours_unavailable": self.twitter_users_aggregates[uid]["total_unavailable_hours"], 
                "account_suspended": self.twitter_users_aggregates[uid]["account_suspended"], 
                "account_deleted": self.twitter_users_aggregates[uid]["account_deleted"], 
                "account_protected": self.twitter_users_aggregates[uid]["account_protected"], 
            }

            if user_json is not None:
                self.tweet_day_dataframe[uid]["id"] = uid
                self.tweet_day_dataframe[uid]["created_at"] = datetime.datetime.strptime(user_json["created_at"], TWITTER_DATETIME_STR_FORMAT)
                self.tweet_day_dataframe[uid]["user_language"] = user_json["lang"]
                self.tweet_day_dataframe[uid]["user_default_profile"] = user_json["default_profile"] and user_json["default_profile_image"] #???? profile and/or profile_image? 
                self.tweet_day_dataframe[uid]["user_verified"] = user_json["verified"]

                self.user_dataframe[uid]["id"] =  uid
                self.user_dataframe[uid]["created_at"] =  datetime.datetime.strptime(user_json["created_at"], TWITTER_DATETIME_STR_FORMAT)
                self.user_dataframe[uid]["user_language"] =  user_json["lang"]
                self.user_dataframe[uid]["user_default_profile"] =  user_json["default_profile"] and user_json["default_profile_image"] #???? profile and/or profile_image? 
                self.user_dataframe[uid]["user_verified"] =  user_json["verified"]

            self.tweet_day_dataframe[uid]["date_first_notice_received"] = self.twitter_users_to_notice_dates[uid][0] 
            self.tweet_day_dataframe[uid]["notices_received"] = len(self.twitter_users_to_notice_dates[uid]) 

            for i in range(-self.twitter_users_aggregates[uid]["num_days_before_day_0"], self.twitter_users_aggregates[uid]["num_days_after_day_0"]+1):
                self.tweet_day_dataframe[uid]["day_num"] = i 
                self.tweet_day_dataframe[uid]["notices_received_on_day"] = 0 
                self.tweet_day_dataframe[uid]["before_first_notice"] = i < 0 
                self.tweet_day_dataframe[uid]["after_first_notice"] = i > 0
                self.tweet_day_dataframe[uid]["num_tweets"] = 0 
                self.tweet_day_dataframe[uid]["num_media_tweets"] = 0 
                self.tweet_day_dataframe[uid]["hours_unavailable"] = 0 # ???? how to count number of hours during this day?
                self.tweet_day_dataframe[uid]["account_suspended"] = False
                self.tweet_day_dataframe[uid]["account_deleted"] = False 
                self.tweet_day_dataframe[uid]["account_protected"] = False

                if i in self.twitter_users_day_nums[uid]:
                    self.tweet_day_dataframe[uid]["notices_received_on_day"] = self.twitter_users_day_nums[uid][i]["num_notices"]
                    self.tweet_day_dataframe[uid]["before_first_notice"] = i < 0
                    self.tweet_day_dataframe[uid]["after_first_notice"] = i > 0
                    self.tweet_day_dataframe[uid]["num_tweets"] = self.twitter_users_day_nums[uid][i]["num_tweets"] 
                    self.tweet_day_dataframe[uid]["num_media_tweets"] = self.twitter_users_day_nums[uid][i]["num_media_tweets"] 

                    self.tweet_day_dataframe[uid]["account_suspended"] = self.twitter_users_day_nums[uid][i]["suspended"]
                    self.tweet_day_dataframe[uid]["account_deleted"] = self.twitter_users_day_nums[uid][i]["deleted"]
                    self.tweet_day_dataframe[uid]["account_protected"] = self.twitter_users_day_nums[uid][i]["protected"]

                    self.tweet_day_dataframe[uid]["hours_unavailable"] = 24 if (
                        self.twitter_users_day_nums[uid][i]["suspended"] or 
                        self.twitter_users_day_nums[uid][i]["deleted"] or 
                        self.twitter_users_day_nums[uid][i]["protected"]) else 0 # ???? how to count number of hours during this day?



################################################
############## basic_profiling
################################################

# was used for basic_profiling
def append_to_csv(fname, row):
    with open(fname, "a") as f:
        f.write(",".join(row) + "\n")

class TwitterBasicProfilingController:
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