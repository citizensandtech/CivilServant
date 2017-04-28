import twitter
import simplejson as json
import datetime
from app.models import Base, TwitterUser, TwitterStatus, LumenNoticeToTwitterUser, TwitterUserSnapshot
import utils.common
import requests
import sqlalchemy
from sqlalchemy import and_, func
import utils.common
from utils.common import TwitterUserState, SUSPENDED_TWITTER_USER_STR

class TwitterController():
    def __init__(self, db_session, t, log):
        self.t = t
        self.db_session = db_session
        self.log = log    

    """
        for each new username parsed from a lumen notice:
        if user found:
            update lumennoticetouser record
            store twitteruser record
            store twitterusersnapshot record
        if user not found:
            store twitteruserrecord
    """
    def query_and_archive_new_users(self):
        unarchived_notice_users = self.db_session.query(LumenNoticeToTwitterUser).filter(and_(
                LumenNoticeToTwitterUser.CS_account_queried == False,
                LumenNoticeToTwitterUser.twitter_username != SUSPENDED_TWITTER_USER_STR
            ).all())
        unarchived_users = set([nu.twitter_username for nu in unarchived_notice_users])
        all_users_info = self.archive_users(unarchived_users, self.new_found_users_store_func, self.new_not_found_users_store_func, has_ids=False)

        unarchived_notice_users_dict = {nu.twitter_username: nu for nu in unarchived_notice_users}
        for user in all_users_info:
            user_json = json.loads(json.dumps(user._json).encode("utf-8", "replace")) if type(user) is twitter.models.User else user   # to accomodate test fixture data
            screen_name = user_json["screen_name"].lower()
            if screen_name in unarchived_notice_users_dict:
                unarchived_notice_users_dict[screen_name].twitter_user_id = user_json["id"]
                unarchived_notice_users_dict[screen_name].CS_account_queried = True
                unarchived_notice_users_dict.remove(screen_name)
        for screen_name in unarchived_notice_users_dict:
            # for the ones left, still should update CS_account_queried
            unarchived_notice_users_dict[screen_name].CS_account_queried = True
        try:
            self.db_session.commit()
            self.log.info("Updated {0} LumenNoticeToTwitterUser.CS_account_queried fields.".format(len(unarchived_notice_users)))
        except:
            self.log.error("Error while saving DB Session for {0} LumenNoticeToTwitterUser.CS_account_queried fields.".format(len(unarchived_notice_users)))
 


    """
        for each user in twitterusersnapshot with too old most recent snapshot:
        if user found:
            store twitterusersnapshot record
        if user not found:
            store twitterusersnapshot record
    """
    def query_and_archive_user_snapshots_and_tweets(self, min_time):
        need_snapshot_users = self.db_session.query(
            TwitterUserSnapshot.twitter_user_id, func.max(TwitterUserSnapshot.created_at)).filter(
            TwitterUserSnapshot.created_at < min_time).all()

        unarchived_users = set([snapshot.twitter_user_id for snapshot in need_snapshot_users])
        self.archive_users(unarchived_users, self.old_found_users_store_func, self.old_not_found_users_store_func, has_ids=True)
        
        for user_id in unarchived_users:
            self.archive_user_tweets(user_id)


    def query_and_archive_tweets(self):
        unarchived_users = self.db_session.query(TwitterUser).filter(
                TwitterUser.CS_most_tweets_queried == False).all()
        for user in unarchived_users:
            self.archive_user_tweets(user.id)
            user.CS_most_tweets_queried = True
        try:
            self.db_session.commit()
            self.log.info("Updated {0} TwitterUser.CS_most_tweets_queried fields.".format(len(unarchived_users)))
        except:
            self.log.error("Error while saving DB Session for {0} TwitterUser.CS_most_tweets_queried fields.".format(len(unarchived_users)))



    """
    users_info: from twitter.api.UsersLookup
    not_found_users: set of usernames still not found

    returns: updated not_found_users

    """
    def new_found_users_store_func(self, users_info, not_found_users):
        for user in users_info:
            user_json = json.loads(json.dumps(user._json).encode("utf-8", "replace")) if type(user) is twitter.models.User else user   # to accomodate test fixture data
            screen_name = user_json["screen_name"].lower()
            try:
                created_at = datetime.datetime.strptime(user_json["created_at"], "%a %b %d %H:%M:%S %z %Y")
                user_record = TwitterUser(
                    id = user_json["id"],
                    screen_name = screen_name, #usernames change! index/search on id when possible
                    created_at = created_at,   # is UTC; expected string format: "Mon Nov 29 21:18:15 +0000 2010"
                    lang = user_json["lang"],
                    user_state = TwitterUserState.FOUND.value if not user_json["protected"] else TwitterUserState.PROTECTED.value,                            
                    CS_most_tweets_queried = False)
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
    not_found_users: set of usernames still not found
    """
    def new_not_found_users_store_func(self, not_found_users):
        for username in list(not_found_users):
            try:
                user_record = TwitterUser(
                    id =  "{0}_{1}".format(SUSPENDED_TWITTER_USER_STR, utils.common.time_since_epoch_ms(datetime.datetime.utcnow())),
                    screen_name = username.lower(),
                    user_state = TwitterUserState.NOT_FOUND.value)
                self.db_session.add(user_record)
            except:
                self.log.error("Error while creating TwitterUser object for user {0}".format(username))
        try:
            self.db_session.commit()
            self.log.info("Saved {0} not_found twitter users' info.".format(len(not_found_users)))
        except:
            self.log.error("Error while saving DB Session for {0} not_found twitter users' info.".format(len(not_found_users)))


    """
    users_info: from twitter.api.UsersLookup
    not_found_users: set of usernames still not found 

    returns: updated not_found_users

    """
    def old_found_users_store_func(self, users_info, not_found_users):
        for user in users_info:
            user_json = json.loads(json.dumps(user._json).encode("utf-8", "replace")) if type(user) is twitter.models.User else user   # to accomodate test fixture data
            screen_name = user_json["screen_name"].lower()
            try:
                created_at = datetime.datetime.strptime(user_json["created_at"], "%a %b %d %H:%M:%S %z %Y")

                # create TwitterUserSnapshot record
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

                not_found_users.discard(screen_name) # discard doesn't throw an error
            except:
                self.log.error("Error while creating TwitterUserSnapshot object for user {0}".format(screen_name))
        try:
            self.db_session.commit()
            self.log.info("Saved {0} found twitter users' info.".format(len(users_info)))
        except:
            self.log.error("Error while saving DB Session for {0} found twitter users' info.".format(len(users_info)))

        return not_found_users

    """
    not_found_users: set of usernames still not found
    """
    def old_not_found_users_store_func(self, not_found_users):
        # we expect not found old users if an existing user (with TwitterUser and 
        # TwitterUserSnapshot records) is deleted/suspended...
        # we still want to keep snapshots
        for user_id in list(not_found_users):
            try:
                if not self.db_session.query(TwitterUser).filter(TwitterUser.screen_name == user_id).first():
                    user_snapshot_record = TwitterUserSnapshot(
                        twitter_user_id = user_id,
                        created_at = created_at,   # is UTC; expected string format: "Mon Nov 29 21:18:15 +0000 2010"
                        # should we have thses???
                        statuses_count = None,
                        followers_count = None,
                        friends_count = None,
                        verified = None,
                        user_state = TwitterUserState.NOT_FOUND.value,
                        user_json = None)
                    self.db_session.add(user_snapshot_record)


            except:
                self.log.error("Error while creating TwitterUserSnapshot object for user {0}".format(user_id))
        try:
            self.db_session.commit()
            self.log.info("Saved {0} not_found twitter users' info.".format(len(not_found_users)))
        except:
            self.log.error("Error while saving DB Session for {0} not_found twitter users' info.".format(len(not_found_users)))


    # unarchived_users is a set
    def archive_users(self, unarchived_users, found_users_store_func, not_found_users_store_func, has_ids=False):
        if len(unarchived_users) <= 0:
            return None

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
                    if has_ids:
                        users_info = self.t.api.UsersLookup(user_id=this_users)
                    else: 
                        users_info = self.t.api.UsersLookup(screen_name=this_users)
                    self.log.info("Queried for {0} Twitter users out of a total of {1} users, got {2} users".format(
                        limit-prev_limit, len(users), len(users_info)))
                except twitter.error.TwitterError as e:
                    self.log.error("Failed to query for Twitter users using api.UsersLookup: {0}".format(str(e)))
                prev_limit = limit

                # for found users, commit to db
                not_found_users = found_users_store_func(users_info, not_found_users)
                all_users_info += users_info

        # TODO: at the end, for not found users, determine suspended or deleted in a better way than right now.
        # right now, we can tell when we call self.archive_user_tweets(user)
        not_found_users_store_func(not_found_users)

        if not has_ids:
            return all_users_info


    def archive_user_tweets(self, user_id):
        query_newest_tweet_stored_time = self.db_session.query(
            func.max(TwitterStatus.created_at)).filter(
            TwitterStatus.user_id == user_id).first()

        # probably a bit redundant but can fix later
        newest_tweet_stored_time = query_newest_tweet_stored_time if query_newest_tweet_stored_time is None else query_newest_tweet_stored_time[0]
        max_id_queried = None   # oldest seen ever from GetUserTimeline
        oldest_id_queried = None   # oldest seen in each GetUserTimeline batch
        seen_statuses = set([])
        try:
            while True:
                prev_seen_statuses_length = len(seen_statuses)
                statuses = self.t.api.GetUserTimeline(user_id=user_id, count=200, max_id=max_id_queried)

                if statuses:
                    self.log.info("Queried total of {0} tweets for account {1}".format(len(statuses), user_id))
                    statuses_jsons = [json.loads(json.dumps(status._json).encode("utf-8", "replace")) if type(status) is twitter.models.Status else status for status in statuses] # to accomodate test fixture data]
                    sorted_statuses_jsons = sorted(statuses_jsons, key=lambda s: datetime.datetime.strptime(s["created_at"], "%a %b %d %H:%M:%S %z %Y"))
                    for i, status_json in enumerate(sorted_statuses_jsons):
                        self.log.info("{0}: {1}".format(i, status_json["created_at"]))
                        status_id = status_json["id"]
                        created_at = datetime.datetime.strptime(status_json["created_at"], "%a %b %d %H:%M:%S %z %Y")
                        # if status hasn't been stored before, store
                        if not newest_tweet_stored_time or created_at > newest_tweet_stored_time:
                            self.log.info("^saved")
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
                        newest_tweet_stored_time = max(created_at, newest_tweet_stored_time) if newest_tweet_stored_time else created_at
                        oldest_id_queried = min(oldest_id_queried, status_id) if oldest_id_queried else status_id
                else:
                    self.log.error("Unexpected error while calling api.GetUserTimeline on user_id {0}: nothing returned".format(user_id))

                try:
                    self.db_session.commit()
                    self.log.info("Saved {0} statuses for user {1}.".format(len(seen_statuses) - prev_seen_statuses_length, user_id))
                except:
                    self.log.error("Error while saving DB Session for {0} statuses for user {1}.".format(len(seen_statuses) - prev_seen_statuses_length, user_id))

                if len(statuses) == 0 or prev_seen_statuses_length == len(seen_statuses):
                    break
                if max_id_queried is None or oldest_id_queried < max_id_queried:
                    max_id_queried = oldest_id_queried
                else:
                    break
        except twitter.error.TwitterError as e:
            self.log.info(e)
            # TODO: un-jankify this error handling/parsing code
            state = None
            if e.message == "Not authorized.": 
                # Account is either protected or suspended
                state = TwitterUserState.SUSPENDED.value
            elif e.message[0]['code'] == 34: # message = "Sorry, that page does not exist."
                state = TwitterUserState.NOT_FOUND.value
            else:
                self.log.error("Unexpected exception while calling api.GetUserTimeline on user {0}: {1}".format(screen_name, e))

            if state:
                # TODO: this is updating TwitterUser object. should we instead update the snapshots?
                queried_user = self.db_session.query(TwitterUser).filter(TwitterUser.screen_name == screen_name).first()    # record should exist
                if queried_user.user_state is not TwitterUserState.PROTECTED.value: 
                    # if user is protected, this update will not have been helpful
                    queried_user.user_state = state
                try:
                    self.db_session.commit()
                    self.log.info("Updated twitter user {0}'s state to {1}.".format(screen_name, state))
                except:
                    self.log.error("Error while saving DB Session")

