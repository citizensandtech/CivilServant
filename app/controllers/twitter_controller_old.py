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

TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"

class TwitterController():
    def __init__(self, db_session, t, log):
        self.t = t
        self.db_session = db_session
        self.log = log    

    """
        for each new username parsed from a lumen notice:
            update lumennoticetouser record
                update CS_user_archived field
                update twitter user id
            store twitteruserrecord
            store 1st twitterusersnapshot
    """
    def query_and_archive_new_users(self):
        unarchived_notice_users = self.db_session.query(
                LumenNoticeToTwitterUser).filter(and_(
                LumenNoticeToTwitterUser.CS_account_queried == CS_JobState.NOT_PROCESSED,
                LumenNoticeToTwitterUser.twitter_username != NOT_FOUND_TWITTER_USER_STR)).all()
        unarchived_users = set([nu.twitter_username for nu in unarchived_notice_users])

        utils.common.update_CS_JobState(unarchived_notice_users, "CS_account_queried", CS_JobState.IN_PROGRESS)

        (all_users_info, noticeuser_to_state) = self.archive_new_users(unarchived_users)

        for noticeuser in noticeuser_to_state:


        #########
        ##row_to_state = self.archive_users(unarchived_users, self.new_found_users_store_func, self.new_not_found_users_store_func, has_ids=False)
        #
        #########
        #unarchived_notice_users_dict = {nu.twitter_username: nu for nu in unarchived_notice_users}
        #for user in all_users_info:
        #    user_json = json.loads(json.dumps(user._json).encode("utf-8", "replace")) if type(user) is twitter.models.User else user   # to accomodate test fixture data
        #    screen_name = user_json["screen_name"].lower()
        #    if screen_name in unarchived_notice_users_dict:
        #        unarchived_notice_users_dict[screen_name].twitter_user_id = user_json["id"]
        #        unarchived_notice_users_dict[screen_name].CS_account_queried = True
        #        unarchived_notice_users_dict.pop(screen_name)
        #for screen_name in unarchived_notice_users_dict:
        #    # for the ones left, still should update CS_account_queried
        #    unarchived_notice_users_dict[screen_name].CS_account_queried = True
        #try:
        #    self.db_session.commit()
        #    self.log.info("Updated {0} LumenNoticeToTwitterUser.CS_account_queried fields.".format(len(unarchived_notice_users)))
        #except:
        #    self.log.error("Error while saving DB Session for {0} LumenNoticeToTwitterUser.CS_account_queried fields.".format(len(unarchived_notice_users)))
 


    """
        for each user in twitterusersnapshot with too old most recent snapshot:
            store twitterusersnapshot record
            update twitteruser?
            store tweets?
    """
    def query_and_archive_user_snapshots_and_tweets(self, min_time, prioritize_new_users):
        need_snapshot_users = self.db_session.query(
            TwitterUserSnapshot.twitter_user_id, func.max(TwitterUserSnapshot.created_at)).filter(
            TwitterUserSnapshot.created_at < min_time).all()

        need_snapshot_user_ids_set = set([us.twitter_user_id for us in need_snapshot_users])
        need_snapshot_user_ids = list(need_snapshot_user_ids_set)
        new_need_snapshot_user_ids_set = set([])
        new_need_snapshot_user_ids = []
        if prioritize_new_users:
            # TODO: is this this the desired behavior?
            new_need_snapshot_users = self.db_session.query(TwitterUser).filter(
                TwitterUser.id.in_(need_snapshot_user_ids)).filter(
                TwitterUser.CS_most_tweets_queried == False).all()
            new_need_snapshot_user_ids_set = set([us.twitter_user_id for us in new_need_snapshot_users])
            new_need_snapshot_user_ids = list(new_need_snapshot_user_ids_set)

            # call on new users first
            (noticeuser_to_state, all_users_info) = self.archive_old_users(new_need_snapshot_user_ids)
            self.with_user_records_archive_tweets(new_need_snapshot_users)  # TwitterUsers

        need_snapshot_user_ids = need_snapshot_user_ids - new_need_snapshot_user_ids
        (noticeuser_to_state, all_users_info) = self.archive_old_users(new_need_snapshot_user_ids)
        self.with_user_records_archive_tweets(new_need_snapshot_users)


    def with_user_records_archive_tweets(self, user_records):
        utils.common.update_CS_JobState(user_records, "CS_most_tweets_queried", CS_JobState.IN_PROGRESS)

        user_to_state = {}
        for user in user_records:
            state = self.archive_user_tweets(user.id)
            user_to_state[user] = state

        utils.common.update_all_CS_JobState(user_to_state, "CS_most_tweets_queried")


    def query_and_archive_tweets(self):
        unarchived_users = self.db_session.query(TwitterUser).filter(
                TwitterUser.CS_most_tweets_queried == False).all()

        self.with_user_records_archive_tweets(unarchived_users)

    def get_users_by_id(self, user_ids):
        return self.t.api.UsersLookup(user_id=user_ids)

    def get_users_by_name(self, user_names):
        return self.t.api.UsersLookup(screen_name=user_names)

    """
        for each new username parsed from a lumen notice:
            (update lumennoticetouser record, not in this function)
            store twitteruserrecord
            store 1st twitterusersnapshot

        return not_found_users, record_to_state
    """
    def store_new_found_users(self, users_info, not_found_users, record_to_state):
        failed_users = 
        for user in users_info:
            user_json = json.loads(json.dumps(user._json).encode("utf-8", "replace")) if type(user) is twitter.models.User else user   # to accomodate test fixture data
            screen_name = user_json["screen_name"].lower()
            try:
                created_at = datetime.datetime.strptime(user_json["created_at"], TWITTER_DATETIME_STR_FORMAT)
                
                # create TwitterUser record
                user_record = TwitterUser(
                    id = user_json["id"],
                    screen_name = screen_name, #usernames change! index/search on id when possible
                    created_at = created_at,   # is UTC; expected string format: "Mon Nov 29 21:18:15 +0000 2010"
                    account_created_at = datetime.datetime.utcnow(),
                    lang = user_json["lang"],
                    user_state = TwitterUserState.FOUND.value if not user_json["protected"] else TwitterUserState.PROTECTED.value,                            
                    CS_most_tweets_queried = CS_JobState.NOT_PROCESSED)
                self.db_session.add(user_record)


                # also create first TwitterUserSnapshot record
                user_snapshot_record = TwitterUserSnapshot(
                    twitter_user_id = user_json["id"],
                    created_at = datetime.datetime.utcnow(),
                    user_state = TwitterUserState.FOUND.value if not user_json["protected"] else TwitterUserState.PROTECTED.value,
                    user_json = json.dumps(user_json)) #already encoded
                self.db_session.add(user_snapshot_record)

                # also update LumenNoticeToTwitterUser.twitter_user_id field
                ################## TODO

                not_found_users.discard(screen_name) # discard doesn't throw an error
            except:
                self.log.error("Error while creating TwitterUser, TwitterUserSnapshot, LumenNoticeToTwitterUser object for user {0}".format(screen_name))

        try:
            self.db_session.commit()
            self.log.info("Saved {0} found twitter users' info.".format(len(users_info)))
        except:
            self.log.error("Error while saving DB Session for TwitterUser, TwitterUserSnapshot, LumenNoticeToTwitterUser object for user {0}".format(screen_name))

        return not_found_users

    """
        for each new username parsed from a lumen notice:
            (update lumennoticetouser record, not in this function)
            store twitteruserrecord
            store 1st twitterusersnapshot

        return record_to_state

    """
    def store_new_not_found_users(self, not_found_users, record_to_state):
        for username in list(not_found_users):
            try:
                user_record = TwitterUser(
                    id =  "{0}_{1}".format(NOT_FOUND_TWITTER_USER_STR, utils.common.time_since_epoch_ms(datetime.datetime.utcnow())),
                    screen_name = username.lower(),
                    created_at = datetime.datetime.utcnow(),
                    user_state = TwitterUserState.NOT_FOUND.value,
                    CS_oldest_tweets_archived = CS_JobState.NOT_PROCESSED)
                self.db_session.add(user_record)

                # also create first TwitterUserSnapshot record
                user_snapshot_record = TwitterUserSnapshot(
                    twitter_user_id = user_json["id"],
                    created_at = created_at,   # is UTC; expected string format: "Mon Nov 29 21:18:15 +0000 2010"
                    # should we have thses???
                    statuses_count = user_json["statuses_count"],
                    followers_count = user_json["followers_count"],
                    friends_count = user_json["friends_count"],
                    verified = user_json["verified"],
                    user_state = TwitterUserState.FOUND.value if not user_json["protected"] else TwitterUserState.PROTECTED.value,
                    user_json = json.dumps(user_json)) #already encoded
                self.db_session.add(user_snapshot_record)

            except:
                self.log.error("Error while creating TwitterUser object for user {0}".format(username))
        try:
            self.db_session.commit()
            self.log.info("Saved {0} not_found twitter users' info.".format(len(not_found_users)))
        except:
            self.log.error("Error while saving DB Session for {0} not_found twitter users' info.".format(len(not_found_users)))




    def archive_new_users(self, unarchived_users):
        noticeuser_to_state = {}
        return self.archive_users(noticeuser_to_state, unarchived_users, self.get_users_by_id, self.store_new_found_users, self.store_new_not_found_users, has_ids=False)

    def archive_old_users(self, unarchived_users):
        record_to_state = {}
        return self.archive_users(record_to_state, archived_users, self.get_users_by_name, self.store_old_found_users, self.store_old_not_found_users,has_ids=True)

    # unarchived_users is a set
    def archive_users(self, record_to_state, unarchived_users, get_users_func, store_found_users_func, store_not_found_users_func, has_ids=False):
        if len(unarchived_users) <= 0:
            return record_to_state

        batch_size = 100 # limit should be 100
        not_found_users = unarchived_users # not_found_users references unarchived_users
        users = list(unarchived_users) # copy as a list

        all_users_info = []
        
        # query batch_size at a time
        prev_limit = 0
        for i in range(1,int(len(users)/batch_size)+2):
            rows = []
            limit = min(i*batch_size, len(users))
            if limit > prev_limit:
                # query twitter API for user info
                users_info = []
                try:
                    this_users = users[prev_limit:limit]
                    users_info = get_users_func(this_users)
                    all_users_info += users_info
                    prev_limit = limit
                    self.log.info("Queried for {0} Twitter users out of a total of {1} users, got {2} users".format(
                        limit-prev_limit, len(users), len(users_info)))
                except twitter.error.TwitterError as e:
                    self.log.error("Failed to query for Twitter users using api.UsersLookup: {0}".format(str(e)))
                
                # for found users, commit to db
                not_found_users, record_to_state = store_users_func(users_info, not_found_users, record_to_state)
        
        # at end, for not found users, commit to db
        record_to_state = self.store_not_found_users_func(not_found_users, record_to_state)

        return all_users_info, record_to_state








    def archive_user_tweets(self, user_id):
        state = CS_JobState.PROCESSED

        query_oldest_id = self.db_session.query(
            func.max(TwitterStatus.id)).filter(
            TwitterStatus.user_id == user_id).first()

        oldest_id_queried = None if query_oldest_id is None else query_oldest_id[0]
        seen_statuses = set([]) # set of ids added this time
        while True:

            # get statuses from twitter API
            try:
                statuses = self.t.api.GetUserTimeline(user_id=user_id, count=200, max_id=oldest_id_queried)
            except twitter.error.TwitterError as e:
                self.log.info(e)
                # TODO: un-jankify this error handling/parsing code. might not get much better though
                state = None
                if e.message == "Not authorized.": 
                    # Account is either protected or suspended
                    state = TwitterUserState.SUSPENDED.value
                elif e.message[0]['code'] == 34: # message = "Sorry, that page does not exist."
                    state = TwitterUserState.NOT_FOUND.value
                else:
                    self.log.error("Unexpected twitter.error.TwitterError exception while calling api.GetUserTimeline on user {0}: {1}".format(screen_name, e))
                    state = CS_JobState.NEEDS_RETRY

                # if either protected/suspended or not found
                if state:
                    # store TwitterUser
                    queried_user = self.db_session.query(TwitterUser).filter(TwitterUser.screen_name == screen_name).first()    # record should exist
                    if queried_user.user_state is not TwitterUserState.PROTECTED.value: 
                        # if user is protected, this update will not have been helpful
                        queried_user.user_state = state

                    # TODO: store TwitterUserSnapshot to mark this state change


                    try:
                        self.db_session.commit()
                        self.log.info("Updated twitter user {0}'s state to {1}.".format(screen_name, state))
                    except:
                        self.log.error("Error while saving DB Session")
                        state = CS_JobState.FAILED
                break                

            if not statuses:
                self.log.error("Unexpected error while calling api.GetUserTimeline on user_id {0}: nothing returned".format(user_id))
                state = CS_JobState.FAILED
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
                if (not oldest_id_queried) or (status_id > oldest_id_queried) or (status_id not in seen_statuses):
                    try:
                        status_record = TwitterStatus(
                            id = status_id,
                            user_id = status_json["user"]["id"],
                            created_at = created_at, #"Sun Apr 16 17:11:30 +0000 2017"
                            status_data = json.dumps(status_json))
                        self.db_session.add(status_record)
                        seen_statuses.add(status_id)
                    except:
                        self.log.error("Error while creating TwitterStatus object for user {0}, status id {1}".format(status_json["user"]["id"]["screen_name"], status_id))
                        state = CS_JobState.FAILED
            try:
                self.db_session.commit()
                self.log.info("Saved {0} statuses for user {1}.".format(len(seen_statuses) - prev_seen_statuses_length, user_id))
            except:
                self.log.error("Error while saving DB Session for {0} statuses for user {1}.".format(len(seen_statuses) - prev_seen_statuses_length, user_id))
                state = CS_JobState.FAILED
            if prev_seen_statuses_length == len(seen_statuses):
                break
            if oldest_id_queried is None or min(seen_statuses) < oldest_id_queried:
                oldest_id_queried = min(seen_statuses)
            else:
                break

        return state

