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
from utils.common import TwitterUserState, NOT_FOUND_TWITTER_USER_STR
import copy

utc=pytz.UTC
TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"


def append_rows_to_csv(dataframe, header, output_dir, fname):
    with open(os.path.join(output_dir, fname), "a") as f:
        for uid in dataframe:
            if "tweet_day" in fname:    # hacky but works...
                for dn in dataframe[uid]:
                    row = [str(dataframe[uid][dn][label]) for label in header]
                    f.write(",".join(row) + "\n")
            elif "user" in fname:
                row = [str(dataframe[uid][label]) for label in header]
                f.write(",".join(row) + "\n")

def date_to_str(date):
    return "{0}-{1}-{2}".format(date.year, date.month, date.day)

class TwitterObservationalAnalysisController:
    def __init__(self, start_date, end_date, min_observed_days, output_dir, db_session, log):
        self.start_date_utc = utc.localize(start_date) if (start_date.tzinfo is None or start_date.tzinfo.utcoffset(d) is None) else start_date
        self.end_date_utc = utc.localize(end_date) if (end_date.tzinfo is None or end_date.tzinfo.utcoffset(d) is None) else end_date

        self.min_observed_days = min_observed_days
        now = utc.localize(datetime.datetime.utcnow())
        #self.end_date_utc = min(self.end_date_utc, now - datetime.timedelta(days=self.min_observed_days))

        self.output_dir = output_dir
        self.db_session = db_session
        self.log = log

        self.user_ids_to_not_found_ids = {}

        self.tweet_day_header = ["id", "not_found_id", "created_at", "user_language", "user_default_profile", \
            "user_verified", "date_first_notice_received", "notices_received", \
            "notices_received_on_day", "day_num", "before_first_notice", \
            "after_first_notice", "num_tweets", "num_media_tweets", "hours_unavailable", \
            "account_suspended", "account_deleted", "account_protected", "total_tweets", "total_past_tweets"]
        self.tweet_day_fname = "{0}_obs_analysis_tweet_day_{1}-{2}_n={3}.csv".format(
            date_to_str(now), 
            date_to_str(self.start_date_utc), date_to_str(self.end_date_utc),
            self.min_observed_days) 

        self.user_header = ["id", "not_found_id", "created_at", "user_language", "user_default_profile", 
            "user_verified", "date_first_notice_received", "tweets_per_day_before_first_notice", 
            "tweets_per_day_after_first_notice", "hours_unavailable", "account_suspended", 
            "account_deleted", "account_protected", "notices_received", "total_tweets", "total_past_tweets"]
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


    # Returns Two Lists of Dicts, Where Each Dict Contains One Row
    def create_datasets(self):

        # For all lumen notices received in [range of time]
        self.log.info("Getting notice dates...")
        self.twitter_users_to_notice_dates = self.get_users_to_notice_dates()   # {user_id: [notice_date_received_day]}
        self.user_ids_to_not_found_ids = {uid: None for uid in self.twitter_users_to_notice_dates.keys()}

        # Get user info
        # Get twitter users (use first snapshot for user info)
        self.log.info("Getting user snapshots...")
        self.twitter_users_to_snapshots = self.get_users_to_snapshots() # get all snapshots, modifies self.user_ids_to_not_found_ids!!!


        ####        
        uids = [id for id in self.user_ids_to_not_found_ids if "NOT_FOUND" not in id]
        self.log.info("Out of total {0} users, {1} have found ids".format(len(self.user_ids_to_not_found_ids), len(uids)))
        partially_unavailable_ids = {id: self.user_ids_to_not_found_ids[id] for id in uids if self.user_ids_to_not_found_ids[id] is not None}
        self.log.info("Out of {0} found ids, {1} also have corresponding not_found_ids: {2}".format(len(uids), len(partially_unavailable_ids), partially_unavailable_ids))


        # query batch_size at a time in order to update job states more often
        batch_size = 20
        prev_limit = 0
        for i in range(1,int(len(uids)/batch_size)+2):
            limit = min(i*batch_size, len(uids))
            self.log.info("Now about to process users indexed {0}-{1}, out of {2} users".format(prev_limit, limit, len(uids)))
            if limit > prev_limit:
                this_uids = uids[prev_limit:limit]


                #Get tweets
                #Need to do something about tweet deletion
                self.log.info("> Getting user tweets...")        
                self.twitter_users_to_tweets = self.get_users_to_tweets(this_uids)   # {user_id: [tweet_created_at_day]}

                #Calculate day_num relative to 1st notice day
                #Get user state per day, suspended/deleted/protected
                self.log.info("> Getting user day nums...")        
                self.twitter_users_day_nums = self.get_users_day_nums(this_uids)

                #Calculate aggregates
                self.log.info("> Getting user aggregates...")        
                self.twitter_users_aggregates = self.get_aggregates(this_uids)


                self.log.info("> Creating dataframes...")        
                self.create_dataframes(this_uids)

                append_rows_to_csv(self.tweet_day_dataframe, self.tweet_day_header, self.output_dir, self.tweet_day_fname)
                append_rows_to_csv(self.user_dataframe, self.user_header, self.output_dir, self.user_fname)

                self.tweet_day_dataframe = {}
                self.user_dataframe = {}

                prev_limit = limit

            self.log.info("Processed {0} out of {1} users".format(prev_limit, len(uids)))


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

            # remove None because that means 
            if uid not in twitter_users_to_notice_dates:
                twitter_users_to_notice_dates[uid] = []
            notice_date = notice_id_to_date[ntu.notice_id]
            twitter_users_to_notice_dates[uid].append(notice_date)

        self.log.info("Retrieved {0} TwitterUser.".format(len(twitter_users_to_notice_dates)))        
        return twitter_users_to_notice_dates

    def get_users_to_snapshots(self):
        twitter_users_to_snapshots = {} # {user_id: [snapshot]}
        num_snapshots = 0
        for uid in self.user_ids_to_not_found_ids:
            snapshots = self.db_session.query(TwitterUserSnapshot).filter(
                or_(TwitterUserSnapshot.twitter_user_id == uid,
                    TwitterUserSnapshot.twitter_not_found_id == uid)).order_by(
                TwitterUserSnapshot.record_created_at).all()


            for snapshot in snapshots:
                if snapshot.twitter_user_id and snapshot.twitter_not_found_id and snapshot.twitter_user_id != snapshot.twitter_not_found_id:
                    if snapshot.twitter_not_found_id==uid and NOT_FOUND_TWITTER_USER_STR in uid:
                        # only 2 class attributes that are set before get_users_to_snapshots is run:
                        self.user_ids_to_not_found_ids.pop(uid, None)
                        self.user_ids_to_not_found_ids[snapshot.twitter_user_id] = uid

                        self.twitter_users_to_notice_dates[snapshot.twitter_user_id] = copy.deepcopy(self.twitter_users_to_notice_dates[uid])
                        self.twitter_users_to_notice_dates.pop(uid, None)

                        uid = snapshot.twitter_user_id

                    elif snapshot.twitter_user_id==uid:
                        if self.user_ids_to_not_found_ids[uid] is not None and self.user_ids_to_not_found_ids[uid] != snapshot.twitter_not_found_id:
                            self.log.info("Unexpected multiple-id-switches= uid: {0}, not_found: {1}, new_not_found: {2}".format(
                                uid, self.user_ids_to_not_found_ids[uid], snapshot.twitter_not_found_id))
                        self.user_ids_to_not_found_ids[uid] = snapshot.twitter_not_found_id
                        # and this doesn't change self.twitter_users_to_notice_dates keys

            twitter_users_to_snapshots[uid] = snapshots
            num_snapshots += len(snapshots)

        self.log.info("Retrieved {0} TwitterUserSnapshot.".format(num_snapshots))
        return twitter_users_to_snapshots

    def get_users_to_tweets(self, this_uids):
        twitter_users_to_tweets = {}    # {user_id: [tweet_created_at_day]}
        
        ###
        batch_size = 20/2

        num_tweets = 0
        for (i, uid) in enumerate(this_uids):
            tweets = self.db_session.query(TwitterStatus).filter(
                or_(TwitterStatus.user_id == uid,
                    TwitterStatus.user_id == self.user_ids_to_not_found_ids[uid])).all()
            twitter_users_to_tweets[uid] = tweets
            num_tweets += len(tweets)
            if i % batch_size == 0 and i != 0:
                self.log.info("...{0} of {1} users; retrieved {2} TwitterStatus so far".format(i, len(this_uids), num_tweets))
        self.log.info("Retrieved {0} TwitterStatus.".format(num_tweets))
        return twitter_users_to_tweets

    def get_users_day_nums(self, this_uids):
        #??? should we remove day_nums that are too large/small?
        twitter_users_day_nums = {}
        for uid in this_uids:
            twitter_users_day_nums[uid] = {}
            first_notice_date = self.twitter_users_to_notice_dates[uid][0]
            day_0 = datetime.datetime(first_notice_date.year, first_notice_date.month, first_notice_date.day)

            # num_notices
            for notice_date in self.twitter_users_to_notice_dates[uid]:
                day_num = (notice_date - day_0).days
                if day_num not in twitter_users_day_nums[uid]:
                    twitter_users_day_nums[uid][day_num] = copy.deepcopy(self.default_day_num_dict_template)
                twitter_users_day_nums[uid][day_num]["num_notices"] += 1

            # num_tweets, num_media_tweets 
            tweets = self.twitter_users_to_tweets[uid] if uid in self.twitter_users_to_tweets else []
            for tweet in tweets:
                day_num = (tweet.created_at - day_0).days
                if day_num not in twitter_users_day_nums[uid]:                 
                    twitter_users_day_nums[uid][day_num] = copy.deepcopy(self.default_day_num_dict_template)
                twitter_users_day_nums[uid][day_num]["num_tweets"] += 1

                # look for media entities
                # https://dev.twitter.com/overview/api/entities-in-twitter-objects
                status_data_json = json.loads(tweet.status_data)
                if "entities" in status_data_json and "media" in status_data_json["entities"]:
                    twitter_users_day_nums[uid][day_num]["num_media_tweets"] += 1                    

            # suspended, deleted, protected
            for snapshot in self.twitter_users_to_snapshots[uid]:
                day_num = (snapshot.record_created_at - day_0).days

                if day_num not in twitter_users_day_nums[uid]:
                    twitter_users_day_nums[uid][day_num] = copy.deepcopy(self.default_day_num_dict_template)
                
                twitter_users_day_nums[uid][day_num]["deleted"] = (snapshot.user_state == TwitterUserState.NOT_FOUND.value)
                twitter_users_day_nums[uid][day_num]["suspended"] = (snapshot.user_state == TwitterUserState.SUSPENDED.value)
                twitter_users_day_nums[uid][day_num]["protected"] = (snapshot.user_state == TwitterUserState.PROTECTED.value)                  

        return twitter_users_day_nums

    def get_aggregates(self, this_uids, prune=True):
        #Calculate aggregates
        #prune ids. ???? should we do this?
        qualifying_user_ids = set([])
        twitter_users_aggregates = {}
        for uid in this_uids:
            aggregates = {
                "total_unavailable_hours": 0,   # how to calculate this ????
                "num_days_before_day_0": 0,
                "num_days_after_day_0": 0,
                "ave_tweets_before_day_0": 0,
                "ave_tweets_after_day_0": 0,
                "total_tweets": 0,
                "account_suspended": False, # ever
                "account_deleted": False, # ever
                "account_protected": False, # ever         
            }

            this_day_nums = [dn for dn in self.twitter_users_day_nums[uid].keys()]
            if min(this_day_nums) <= -self.min_observed_days and max(this_day_nums) >= self.min_observed_days:
                qualifying_user_ids.add(uid)
            else:
                # not best design...
                self.user_ids_to_not_found_ids.pop(uid, None) 

            if (prune and uid in qualifying_user_ids) or (not prune):
                # prune this_day_nums so that aggregate calculations are accurate
                if prune:
                    this_day_num_dicts = {dn: self.twitter_users_day_nums[uid][dn] for dn in self.twitter_users_day_nums[uid] if dn >= -self.min_observed_days and dn <= self.min_observed_days}
                else:
                    this_day_num_dicts = self.twitter_users_day_nums[uid]

                aggregates["num_days_before_day_0"] = max(0, 0 - min(this_day_num_dicts)) if len(this_day_num_dicts) > 0 else 0
                aggregates["num_days_after_day_0"] = max(0, max(this_day_num_dicts)) if len(this_day_num_dicts) > 0 else 0

                aggregates["total_unavailable_hours"] = 24*sum([1 for dn in this_day_num_dicts if (this_day_num_dicts[dn]["suspended"] or this_day_num_dicts[dn]["deleted"] or this_day_num_dicts[dn]["protected"])])

                ###### why do these 2 fields have identical values??? #####
                aggregates["ave_tweets_before_day_0"] = round(sum([this_day_num_dicts[dn]["num_tweets"] for dn in this_day_num_dicts if dn < 0]) / aggregates["num_days_before_day_0"], 2) if aggregates["num_days_before_day_0"] > 0 else 0 
                aggregates["ave_tweets_after_day_0"] = round(sum([this_day_num_dicts[dn]["num_tweets"] for dn in this_day_num_dicts if dn > 0]) / aggregates["num_days_after_day_0"], 2) if aggregates["num_days_after_day_0"] > 0 else 0
                aggregates["total_tweets"] = sum([this_day_num_dicts[dn]["num_tweets"] for dn in this_day_num_dicts])

                aggregates["account_suspended"] = any([this_day_num_dicts[dn]["suspended"] for dn in this_day_num_dicts])
                aggregates["account_deleted"] = any([this_day_num_dicts[dn]["deleted"] for dn in this_day_num_dicts])
                aggregates["account_protected"] = any([this_day_num_dicts[dn]["protected"] for dn in this_day_num_dicts])                        

                twitter_users_aggregates[uid] = aggregates

        self.log.info("Pruned user_ids (prune={3}). {0} qualifying user_ids from {1} in this batch, {2} total.".format(len(qualifying_user_ids), len(this_uids), len(self.user_ids_to_not_found_ids), prune))
        
        return twitter_users_aggregates

    def create_dataframes(self, this_uids):
        for uid in this_uids:
            if uid not in self.user_ids_to_not_found_ids:
                # was pruned
                continue

            user_json = None
            for snapshot in self.twitter_users_to_snapshots[uid]:
                if snapshot.user_json and snapshot.user_json != "":
                    user_json = json.loads(snapshot.user_json)
                    break

            self.tweet_day_dataframe[uid] = {}
            user_tweet_day = {
                "id": uid, 
                "not_found_id": self.user_ids_to_not_found_ids[uid],
                "created_at": None, 
                "user_language": None, 
                "user_default_profile": None, 
                "user_verified": None, 
                "date_first_notice_received": None, 
                "notices_received": 0, 
                "notices_received_on_day": 0, 
                "day_num": None, 
                "before_first_notice": None, 
                "after_first_notice": None, 
                "total_tweets": self.twitter_users_aggregates[uid]["total_tweets"],
                "total_past_tweets": 0,                
                "num_tweets": 0, 
                "num_media_tweets": 0, 
                "hours_unavailable": 0, 
                "account_suspended": False, 
                "account_deleted": False, 
                "account_protected": False            
            }

            self.user_dataframe[uid] = {label: None for label in self.user_header}            
            self.user_dataframe[uid] = {
                "id": uid,
                "not_found_id": self.user_ids_to_not_found_ids[uid],                
                "created_at": None,
                "user_language": None,
                "user_default_profile": None,
                "user_verified": None,
                "date_first_notice_received": self.twitter_users_to_notice_dates[uid][0],
                "notices_received": len(self.twitter_users_to_notice_dates[uid]),
                "tweets_per_day_before_first_notice": self.twitter_users_aggregates[uid]["ave_tweets_before_day_0"], 
                "tweets_per_day_after_first_notice": self.twitter_users_aggregates[uid]["ave_tweets_after_day_0"], 
                "total_tweets": self.twitter_users_aggregates[uid]["total_tweets"],
                "hours_unavailable": self.twitter_users_aggregates[uid]["total_unavailable_hours"], 
                "account_suspended": self.twitter_users_aggregates[uid]["account_suspended"], 
                "account_deleted": self.twitter_users_aggregates[uid]["account_deleted"], 
                "account_protected": self.twitter_users_aggregates[uid]["account_protected"], 
            }


            if user_json is not None:
                user_tweet_day["created_at"] = datetime.datetime.strptime(user_json["created_at"], TWITTER_DATETIME_STR_FORMAT)
                user_tweet_day["user_language"] = user_json["lang"]
                user_tweet_day["user_default_profile"] = user_json["default_profile_image"] 
                user_tweet_day["user_verified"] = user_json["verified"]
                user_tweet_day["total_past_tweets"] = user_json["statuses_count"]

                self.user_dataframe[uid]["created_at"] =  datetime.datetime.strptime(user_json["created_at"], TWITTER_DATETIME_STR_FORMAT)
                self.user_dataframe[uid]["user_language"] =  user_json["lang"]
                self.user_dataframe[uid]["user_default_profile"] =  user_json["default_profile_image"] 
                self.user_dataframe[uid]["user_verified"] =  user_json["verified"]
                self.user_dataframe[uid]["total_past_tweets"] =  user_json["statuses_count"]

            user_tweet_day["date_first_notice_received"] = self.twitter_users_to_notice_dates[uid][0] 
            user_tweet_day["notices_received"] = len(self.twitter_users_to_notice_dates[uid]) 

            # only include day_nums of interest
            for i in range(-self.min_observed_days, self.min_observed_days+1):
                this_user_tweet_day = copy.deepcopy(user_tweet_day)
                this_user_tweet_day["day_num"] = i 
                this_user_tweet_day["before_first_notice"] = i < 0 
                this_user_tweet_day["after_first_notice"] = i > 0

                this_user_tweet_day["hours_unavailable"] = 0 # ???? how to count number of hours during this day?  
                if i in self.twitter_users_day_nums[uid]:
                    this_user_tweet_day["notices_received_on_day"] = self.twitter_users_day_nums[uid][i]["num_notices"]
                    this_user_tweet_day["num_tweets"] = self.twitter_users_day_nums[uid][i]["num_tweets"] 
                    this_user_tweet_day["num_media_tweets"] = self.twitter_users_day_nums[uid][i]["num_media_tweets"] 

                    this_user_tweet_day["account_suspended"] = self.twitter_users_day_nums[uid][i]["suspended"]
                    this_user_tweet_day["account_deleted"] = self.twitter_users_day_nums[uid][i]["deleted"]
                    this_user_tweet_day["account_protected"] = self.twitter_users_day_nums[uid][i]["protected"]

                    this_user_tweet_day["hours_unavailable"] = 24 if (self.twitter_users_day_nums[uid][i]["suspended"] or self.twitter_users_day_nums[uid][i]["deleted"] or self.twitter_users_day_nums[uid][i]["protected"]) else 0 # ???? how to count number of hours during this day?  
                
                self.tweet_day_dataframe[uid][i] = this_user_tweet_day

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
