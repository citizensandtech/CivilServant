import twitter
import simplejson as json
import datetime
from app.models import Base, TwitterUser, TwitterStatus, LumenNoticeToTwitterUser, TwitterUserSnapshot
import utils.common
import requests
import sqlalchemy
from sqlalchemy import and_, func
import utils.common
from utils.common import TwitterUserState, NOT_FOUND_TWITTER_USER_STR, CS_JobState
import sys

TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"

def generate_not_found_twitter_user_id(screen_name=""):
    capped_screen_name = screen_name if len(screen_name)<30 else screen_name[:30] + "..."
    return "{0}_{1}_{2}".format(NOT_FOUND_TWITTER_USER_STR, capped_screen_name, utils.common.time_since_epoch_ms(datetime.datetime.utcnow()))

class TwitterController():
    def __init__(self, db_session, t, log):
        self.t = t
        self.db_session = db_session
        self.log = log    

    #########################################################   
    ################### ARCHIVE NEW USERS CODE
    #########################################################

    def query_and_archive_new_users(self):
        unarchived_notice_users = self.db_session.query(LumenNoticeToTwitterUser).filter(
                LumenNoticeToTwitterUser.CS_account_archived == CS_JobState.NOT_PROCESSED.value).all()

        utils.common.update_CS_JobState(unarchived_notice_users, "CS_account_archived", CS_JobState.IN_PROGRESS, self.db_session, self.log)

        (user_name_to_id, noticeuser_to_state) = self.archive_new_users(unarchived_notice_users)

        # update LumenNoticeToTwitterUser records
        if user_name_to_id and noticeuser_to_state:
            for noticeuser in noticeuser_to_state:
                noticeuser.CS_account_archived = noticeuser_to_state[noticeuser].value
                noticeuser.twitter_user_id = user_name_to_id[noticeuser.twitter_username]
            try:
                self.db_session.commit()
                self.log.info("Updated {0} LumenNoticeToTwitterUser.CS_account_archived,twitter_user_id fields.".format(len(noticeuser_to_state)))
            except:
                self.log.error("Error while saving DB Session for {0} LumenNoticeToTwitterUser.CS_account_archived,twitter_user_id fields.".format(
                    len(noticeuser_to_state)), extra=sys.exc_info()[0])


    def archive_new_users(self, unarchived_notice_users):
        is_test = type(unarchived_notice_users[0]) is not LumenNoticeToTwitterUser
        if len(unarchived_notice_users) <= 0:
            return (None, None)
        unarchived_user_names = set([nu.twitter_username for nu in unarchived_notice_users]) if not is_test else set(unarchived_notice_users) # to accomodate tests...
        user_names = list(unarchived_user_names)

        # to return
        user_name_to_id = {name: None for name in user_names}
        noticeuser_to_state = {nu: CS_JobState.FAILED for nu in unarchived_notice_users} if not is_test else {} # to accomodate tests....
        failed_users = set([])

        # query batch_size at a time
        batch_size = 100 # limit should be 100
        prev_limit = 0
        left_users = unarchived_user_names # reference

        for i in range(1,int(len(user_names)/batch_size)+2):
            rows = []
            limit = min(i*batch_size, len(user_names))
            if limit > prev_limit:
                # query twitter API for user info
                users_info = []
                this_users = user_names[prev_limit:limit]
                try:
                    users_info = self.t.api.UsersLookup(screen_name=this_users)
                    self.log.info("Queried for {0} Twitter users out of a total of {1} users, got {2} users".format(
                        limit-prev_limit, len(user_names), len(users_info)))
                    prev_limit = limit
                except twitter.error.TwitterError as e:
                    failed_users.update(this_users)
                    self.log.error("Failed to query for Twitter users using api.UsersLookup: {0}".format(str(e)))


                # for found users, commit to db
                all_user_names = []
                for user_info in users_info:
                    user_json = json.loads(json.dumps(user_info._json).encode("utf-8", "replace")) if type(user_info) is twitter.models.User else user_info   # to accomodate test fixture data
                    all_user_names.append(user_json["screen_name"])
                    screen_name = user_json["screen_name"].lower()
                    uid = user_json["id"]
                    created_at = datetime.datetime.strptime(user_json["created_at"], TWITTER_DATETIME_STR_FORMAT)

                    # determine user state
                    user_state = TwitterUserState.FOUND if not user_json["protected"] else TwitterUserState.PROTECTED
                    if user_state == TwitterUserState.PROTECTED:
                        # disambiguate with user timeline call. throw away statuses, job_state
                        (statuses, t_user_state, job_state) = self.get_statuses_user_state(self, user_id, count=1)
                        self.log.info(t_user_state)
                        if t_user_state and t_user_state is not TwitterUserState.PROTECTED:
                            user_state = t_user_state
    
                    user_name_to_id[screen_name] = uid

                    try:
                        # create TwitterUser record
                        user_record = TwitterUser(
                            id = uid,
                            screen_name = screen_name, #usernames change! index/search on id when possible
                            created_at = created_at,   # is UTC; expected string format: "Mon Nov 29 21:18:15 +0000 2010"
                            record_created_at = datetime.datetime.utcnow(),
                            lang = user_json["lang"],
                            user_state = user_state.value,                
                            CS_oldest_tweets_archived = CS_JobState.NOT_PROCESSED.value)
                        self.db_session.add(user_record)

                        # create first TwitterUserSnapshot record
                        user_snapshot_record = TwitterUserSnapshot(
                            twitter_user_id = uid,
                            record_created_at = datetime.datetime.utcnow(),
                            user_state = user_state.value,
                            user_json = json.dumps(user_json)) #already encoded
                        self.db_session.add(user_snapshot_record)

                        left_users.discard(screen_name) # discard doesn't throw an error
                    except:
                        self.log.error("Error while updating TwitterUser, creating TwitterUserSnapshot object for user {0}".format(user_json["id"]))
                        failed_users.add(screen_name)
                try:
                    self.db_session.commit()
                    self.log.info("Saved {0} found twitter users' info.".format(len(users_info)))
                except:
                    self.log.error("Error while saving DB Session for TwitterUser, TwitterUserSnapshot object for {0} users".format(
                        len(users_info)), extra=sys.exc_info()[0])
                    failed_users.update(all_user_names)


        # at end, for left_users (users not found), commit to db
        for name in left_users:
            uid = generate_not_found_twitter_user_id(name)
            user_name_to_id[name] = uid

            try:
                # create TwitterUser record
                user_record = TwitterUser(
                    id =  uid,
                    screen_name = name,
                    created_at = None,
                    record_created_at = datetime.datetime.utcnow(),
                    lang = None,
                    user_state = TwitterUserState.NOT_FOUND.value,                
                    CS_oldest_tweets_archived = CS_JobState.NOT_PROCESSED.value)
                self.db_session.add(user_record)

                # also create first TwitterUserSnapshot record
                user_snapshot_record = TwitterUserSnapshot(
                    twitter_user_id = uid,
                    record_created_at = datetime.datetime.utcnow(),
                    user_state = TwitterUserState.NOT_FOUND.value,
                    user_json = None)
                self.db_session.add(user_snapshot_record)

            except:
                self.log.error("Error while updating TwitterUser, creating TwitterUserSnapshot object for user {0}".format(user_json["id"]))
                failed_users.update(name)
        try:
            self.db_session.commit()
            self.log.info("Saved {0} not_found twitter users' info.".format(len(left_users)))
        except:
            self.log.error("Error while saving DB Session for {0} not_found twitter users' info.".format(
                len(left_users)), extra=sys.exc_info()[0])
            failed_users.update(list(left_users))

        for nu in noticeuser_to_state:
            if nu.twitter_username not in failed_users:
                noticeuser_to_state[nu] = CS_JobState.PROCESSED

        return (user_name_to_id, noticeuser_to_state)



    #########################################################   
    ################### ARCHIVE SNAPSHOTS AND NEW TWEETS CODE
    #########################################################

    """
        for each user in twitterusersnapshot with too old most recent snapshot:
            store twitterusersnapshot record
            update twitteruser?
            store tweets?

        doesn't need to update any CS_JobState fields.   
    """
    def query_and_archive_user_snapshots_and_tweets(self, min_time):
        need_snapshot_user_snapshots = self.db_session.query(
            TwitterUserSnapshot.twitter_user_id, func.max(TwitterUserSnapshot.record_created_at)).filter(
            TwitterUserSnapshot.record_created_at < min_time).group_by(TwitterUserSnapshot.twitter_user_id).all()
        need_snapshot_user_ids_set = set([us.twitter_user_id for us in need_snapshot_user_snapshots]) # make sure to get unique ids
        need_snapshot_user_ids = list(need_snapshot_user_ids_set)

        self.log.info("Need to update snapshots for {0} users".format(len(need_snapshot_user_ids_set)))
        if len(need_snapshot_user_ids_set) <= 0:
            return

        need_snapshot_users = self.db_session.query(TwitterUser).filter(
            TwitterUser.id.in_(need_snapshot_user_ids)).all()
        
        # store TwitterUserSnapshot, update TwitterUser for all queried users
        need_snapshot_id_to_user = {u.id: u for u in need_snapshot_users}
        self.archive_old_users(need_snapshot_id_to_user)  # TwitterUsers

        # store new tweets for users with CS_oldest_tweets_archived = PROCESSED
        need_new_tweets_users = [u for u in need_snapshot_users if u.CS_oldest_tweets_archived == CS_JobState.PROCESSED.value]
        self.log.info("Need to get new tweets for {0} users".format(len(need_new_tweets_users)))
        self.with_user_records_archive_tweets(need_new_tweets_users)  # TwitterUsers

    def archive_old_users(self, id_to_users):
        is_test = type(id_to_users) is not dict
        if len(id_to_users) <= 0:
            return None
        user_ids = list(id_to_users.keys()) if not is_test else id_to_users # to accomodate tests....

        if is_test:
            id_to_users = {uid: None for uid in id_to_users}    # to accomodate tests...

        # query batch_size at a time
        batch_size = 100 # limit should be 100
        prev_limit = 0
        left_users = set(user_ids)
        for i in range(1,int(len(user_ids)/batch_size)+2):
            rows = []
            limit = min(i*batch_size, len(user_ids))
            if limit > prev_limit:
                # query twitter API for user info
                users_info = []
                try:
                    this_users = user_ids[prev_limit:limit]
                    users_info = self.t.api.UsersLookup(user_id=this_users)
                    self.log.info("Queried for {0} Twitter users out of a total of {1} users, got {2} users".format(
                        limit-prev_limit, len(user_ids), len(users_info)))
                    prev_limit = limit
                except twitter.error.TwitterError as e:
                    self.log.error("Failed to query for Twitter users using api.UsersLookup: {0}".format(str(e)))
                
                # for found users, commit to db
                for user_info in users_info:
                    user_json = json.loads(json.dumps(user_info._json).encode("utf-8", "replace")) if type(user_info) is twitter.models.User else user_info   # to accomodate test fixture data
                    screen_name = user_json["screen_name"].lower()
                    uid = str(user_json["id"])
                    user = id_to_users[uid] # TwitterUser record

                    user_state = TwitterUserState.FOUND if not user_json["protected"] else TwitterUserState.PROTECTED
                    if user_state == TwitterUserState.PROTECTED:
                        # disambiguate with user timeline call. throw away statuses, job_state
                        (statuses, t_user_state, job_state) = self.get_statuses_user_state(self, user_id, count=1)
                        if t_user_state and t_user_state is not TwitterUserState.PROTECTED:
                            user_state = t_user_state 

                    try:
                        # update TwitterUser record
                        user = id_to_users[uid]
                        user.screen_name = screen_name
                        user.record_updated_at = datetime.datetime.utcnow()
                        user.lang = user_json["lang"]
                        user.state = user_state.value

                        # create first TwitterUserSnapshot record
                        user_snapshot_record = TwitterUserSnapshot(
                            twitter_user_id = uid,
                            record_created_at = datetime.datetime.utcnow(),
                            user_state = user_state.value,
                            user_json = json.dumps(user_json)) #already encoded
                        self.db_session.add(user_snapshot_record)

                        left_users.discard(uid) # discard doesn't throw an error
                    except:
                        self.log.error("Error while updating TwitterUser, creating TwitterUserSnapshot object for user {0}".format(user_json["id"]))

                try:
                    self.db_session.commit()
                    self.log.info("Saved {0} found twitter users' info.".format(len(users_info)))
                except:
                    self.log.error("Error while saving DB Session for TwitterUser, TwitterUserSnapshot object for {0} users".format(
                        len(users_info)), extra=sys.exc_info()[0])

        # at end, for left_users (users not found), commit to db
        for uid in list(left_users):
            try:
                # update TwitterUser record                            
                user = id_to_users[uid]
                user.record_updated_at = datetime.datetime.utcnow()
                user.user_state = TwitterUserState.NOT_FOUND.value

                # create first TwitterUserSnapshot record    
                user_snapshot_record = TwitterUserSnapshot(
                    twitter_user_id = uid,
                    record_created_at = datetime.datetime.utcnow(),
                    user_state = TwitterUserState.NOT_FOUND.value)
                self.db_session.add(user_snapshot_record)

            except:
                self.log.error("Error while updating TwitterUser, creating TwitterUserSnapshot object for user {0}".format(user_json["id"]))
        try:
            self.db_session.commit()
            self.log.info("Saved {0} not_found twitter users' info.".format(len(left_users)))
        except:
            self.log.error("Error while saving DB Session for {0} not_found twitter users' info.".format(
                len(left_users)), extra=sys.exc_info()[0])



    #########################################################   
    ################### ARCHIVE TWEET CODE
    #########################################################

    def query_and_archive_tweets(self):
        unarchived_users = self.db_session.query(TwitterUser).filter(
                TwitterUser.CS_oldest_tweets_archived == CS_JobState.NOT_PROCESSED.value).all()

        self.with_user_records_archive_tweets(unarchived_users)


    def with_user_records_archive_tweets(self, user_records):
        utils.common.update_CS_JobState(user_records, "CS_oldest_tweets_archived", CS_JobState.IN_PROGRESS, self.db_session, self.log)

        user_to_state = {}
        for user in user_records:
            state = self.archive_user_tweets(user.id)
            user_to_state[user] = state

        utils.common.update_all_CS_JobState(user_to_state, "CS_oldest_tweets_archived", self.db_session, self.log)

    # returns (statuses, user_state, job_state)
    def get_statuses_user_state(self, user_id, count=200, max_id=None, job_state=CS_JobState.FAILED):
        (statuses, user_state, job_state) = (None, None, job_state) 
        try:
            statuses = self.t.api.GetUserTimeline(user_id=user_id, count=count, max_id=max_id)
        except twitter.error.TwitterError as e:
            self.log.info(e)
            # TODO: un-jankify this error handling/parsing code. might not get much better though
            state = None
            if e.message == "Not authorized.": 
                # Account is either protected or suspended
                user_state = TwitterUserState.SUSPENDED.value
            elif e.message[0]['code'] == 34: # message = "Sorry, that page does not exist."
                user_state = TwitterUserState.NOT_FOUND.value
            else:
                self.log.error("Unexpected twitter.error.TwitterError exception while calling api.GetUserTimeline on user {0}: {1}".format(screen_name, e))
                job_state = CS_JobState.NEEDS_RETRY
        job_state = CS_JobState.PROCESSED                
        return (statuses, user_state, job_state)


    # given user_id, archive user tweets.
    # also updates TwitterUser record if new user_state info
    def archive_user_tweets(self, user_id):
        job_state = CS_JobState.PROCESSED

        query_oldest_id = self.db_session.query(
            func.max(TwitterStatus.id)).filter(
            TwitterStatus.user_id == user_id).first()

        oldest_id_queried = None if query_oldest_id is None else query_oldest_id[0]
        seen_statuses = set([]) # set of ids added this time
        count = 200
        while True:

            # get statuses and job_state from twitter API. don't use user_state
            (statuses, user_state, job_state) = self.get_statuses_user_state(user_id, count, oldest_id_queried)

            if not statuses:
                self.log.error("Unexpected error while calling api.GetUserTimeline on user_id {0}: nothing returned".format(user_id))
                job_state = CS_JobState.FAILED
                break
            if len(statuses) == 0:
                break
            self.log.info("Queried total of {0} tweets for account {1}".format(len(statuses), user_id))

            # store TwitterStatus es
            statuses_jsons = [json.loads(json.dumps(status._json).encode("utf-8", "replace")) if type(status) is twitter.models.Status else status for status in statuses] # to accomodate test fixture data]
            sorted_statuses_jsons = sorted(statuses_jsons, key=lambda s: datetime.datetime.strptime(s["created_at"], TWITTER_DATETIME_STR_FORMAT))
            prev_seen_statuses_length = len(seen_statuses)
            for i, status_json in enumerate(sorted_statuses_jsons):
                status_id = status_json["id"]
                created_at = datetime.datetime.strptime(status_json["created_at"], TWITTER_DATETIME_STR_FORMAT)
                # if status hasn't been stored before, store
                if ((not oldest_id_queried) or (status_id > oldest_id_queried)) and (status_id not in seen_statuses):
                    try:
                        status_record = TwitterStatus(
                            id = status_id,
                            user_id = str(status_json["user"]["id"]),
                            record_created_at = datetime.datetime.utcnow(),
                            created_at = created_at, #"Sun Apr 16 17:11:30 +0000 2017"
                            status_data = json.dumps(status_json))
                        self.db_session.add(status_record)
                        seen_statuses.add(status_id)
                    except:
                        self.log.error("Error while creating TwitterStatus object for user {0}, status id {1}".format(status_json["user"]["id"]["screen_name"], status_id))
                        job_state = CS_JobState.FAILED
            try:
                self.db_session.commit()
                self.log.info("Saved {0} statuses for user {1}.".format(len(seen_statuses) - prev_seen_statuses_length, user_id))
            except:
                self.log.error("Error while saving DB Session for {0} statuses for user {1}.".format(
                    len(seen_statuses) - prev_seen_statuses_length, user_id), extra=sys.exc_info()[0])
                job_state = CS_JobState.FAILED
            if prev_seen_statuses_length == len(seen_statuses):
                break
            if oldest_id_queried is None or min(seen_statuses) < oldest_id_queried:
                oldest_id_queried = min(seen_statuses)
            else:
                break

        return job_state

