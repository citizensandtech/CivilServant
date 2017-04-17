import twitter
import simplejson as json
import datetime
from app.models import Base, TwitterUser, TwitterStatus 
import utils.common
import requests
import sqlalchemy
from utils.common import TwitterUserState 

class TwitterController():
    def __init__(self, db_session, t, log):
        self.t = t
        self.db_session = db_session
        self.log = log    

    # users_set is a set
    def archive_users(self, users_set, archive_tweets=True):
        # make all Twitter usernames lowercase
        users_set = set([x.lower() for x in list(users_set)])
        existing_users = self.db_session.query(TwitterUser).filter(TwitterUser.screen_name.in_(list(users_set))).all()
        batch_size = 100 # limit should be 100
        new_users_set = users_set - set(existing_users) # is mutated to be set of users still not found
        new_users = list(new_users_set) # is not mutated
        
        # query batch_size at a time
        prev_limit = 0
        for i in range(1,int(len(new_users)/batch_size)+2):
            rows = []
            limit = min(i*batch_size, len(new_users))
            if limit > prev_limit:
                # query twitter API for user info
                users_info = []
                try:
                    users_info = self.t.api.UsersLookup(screen_name=new_users[prev_limit:limit])
                    self.log.info("Queried for {0} Twitter users out of a total of {1} users, got {2} users".format(
                        limit-prev_limit, len(new_users), len(users_info)))
                except twitter.error.TwitterError as e:
                    self.log.error("Failed to query for Twitter users using api.UsersLookup: {0}".format(str(e)))
                prev_limit = limit

                # for found users, commit to db
                for user in users_info:
                    user_json = json.loads(json.dumps(user._json).encode("utf-8", "replace")) if type(user) is twitter.models.User else user   # to accomodate test fixture data
                    screen_name = user_json["screen_name"].lower()
                    try:
                        if not self.db_session.query(TwitterUser).filter(TwitterUser.screen_name == screen_name).first():
                            created_at = datetime.datetime.strptime(user_json["created_at"], "%a %b %d %H:%M:%S %z %Y")
                            user_record = TwitterUser(
                                id = user_json["id"],
                                screen_name = screen_name,
                                name = user_json["name"].encode("utf-8", "replace"), 
                                created_at = created_at,   # is UTC; expected string format: "Mon Nov 29 21:18:15 +0000 2010"
                                followers_count = user_json["followers_count"],
                                friends_count = user_json["friends_count"],
                                lang = user_json["lang"],
                                statuses_count = user_json["statuses_count"],
                                verified = user_json["verified"],
                                default_profile = user_json["default_profile"],
                                default_profile_image = user_json["default_profile_image"],
                                user_json = json.dumps(user_json), #already encoded
                                user_state = TwitterUserState.FOUND.value if not user_json["protected"] else TwitterUserState.PROTECTED.value)
                            self.db_session.add(user_record)
                            new_users_set.discard(screen_name) # discard doesn't throw an error
                    except:
                        self.log.error("Error while creating TwitterUser object for user {0}".format(screen_name))
                try:
                    self.db_session.commit()
                    self.log.info("Saved {0} found twitter users' info.".format(len(users_info)))
                except:
                    self.log.error("Error while saving DB Session")

        # TODO: at the end, for not found users, determine suspended or deleted in a better way than right now.
        # right now, we can tell when we call self.archive_user_tweets(user)
        self.log.info(new_users_set)
        for user in list(new_users_set):
            try:
                if not self.db_session.query(TwitterUser).filter(TwitterUser.screen_name == user).first():
                    user_record = TwitterUser(
                        screen_name = user.lower(),
                        user_state = TwitterUserState.NOT_FOUND.value)
                    self.db_session.add(user_record)
            except:
                self.log.error("Error while creating TwitterUser object for user {0}".format(user))
        try:
            self.db_session.commit()
            self.log.info("Saved {0} not_found twitter users' info.".format(len(new_users_set)))
        except:
            self.log.error("Error while saving DB Session")

        if archive_tweets: 
            for user in new_users: # all users
                self.archive_user_tweets(user)


    # TODO: do we need a method like this that will go through all the users, and archive more tweets?
    def archive_all_users_tweets(self):
        pass

    def archive_user_tweets(self, screen_name):
        max_id = None
        oldest_id_seen = None
        seen_statuses = set([])
        try:
            while True:
                prev_seen_statuses_length = len(seen_statuses)
                statuses = self.t.api.GetUserTimeline(screen_name=screen_name, count=200, max_id=max_id)
                self.log.info("Queried total of {0} tweets for account {1}".format(len(statuses), screen_name))
                for status in statuses:
                    status_json = json.loads(json.dumps(status._json).encode("utf-8", "replace")) if type(status) is twitter.models.Status else status   # to accomodate test fixture data
                    status_id = status_json["id"]

                    # if status hasn't been stored before, store
                    if not self.db_session.query(TwitterStatus).filter(TwitterStatus.id == status_id).first():
                        try:
                            # TODO: should we store anything about entities?
                            status_record = TwitterStatus(
                                id = status_id,
                                user_id = status_json["user"]["id"],
                                in_reply_to_user_id = status_json["favorite_count"],
                                created_at = datetime.datetime.strptime(status_json["created_at"], "%a %b %d %H:%M:%S %z %Y"), #"Sun Apr 16 17:11:30 +0000 2017"
                                favorite_count = status_json["favorite_count"],
                                retweet_count = status_json["retweet_count"],
                                retweeted = status_json["retweeted"],
                                status_data = json.dumps(status_json))
                            self.db_session.add(status_record)
                            seen_statuses.add(status_id)
                        except:
                            self.log.error("Error while creating TwitterStatus object for user {0}, status id {1}".format(status_json["user"]["id"]["screen_name"], status_id))
                    oldest_id_seen = min(oldest_id_seen, status_id) if oldest_id_seen else status_id
                try:
                    self.db_session.commit()
                    self.log.info("Saved {0} statuses for user {1}.".format(len(seen_statuses) - prev_seen_statuses_length, screen_name))
                except:
                    self.log.error("Error while saving DB Session")

                if len(statuses) == 0 or prev_seen_statuses_length == len(seen_statuses):
                    break
                if max_id is None or oldest_id_seen < max_id:
                    max_id = oldest_id_seen
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
                queried_user = self.db_session.query(TwitterUser).filter(TwitterUser.screen_name == screen_name).first()    # record should exist
                if queried_user.user_state is not TwitterUserState.PROTECTED.value: 
                    # if user is protected, this update will not have been helpful
                    queried_user.user_state = state
                try:
                    self.db_session.commit()
                    self.log.info("Updated twitter user {0}'s state to {1}.".format(screen_name, state))
                except:
                    self.log.error("Error while saving DB Session")

