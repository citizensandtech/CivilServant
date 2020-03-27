import math
import random
import datetime

from twitter import TwitterError

from app.controllers.twitter_controller import TwitterController
from app.models import Base, TwitterUser, TwitterStatus, LumenNoticeToTwitterUser, TwitterUserSnapshot, TwitterFill, \
    TwitterUnshortenedUrls, TwitterStatusUrls, ExperimentThing
import sqlalchemy
import utils.common
from utils.common import TwitterUserState, NOT_FOUND_TWITTER_USER_STR, CS_JobState, neq, EXPERIMENT_LANGUAGES, \
    TwitterUrlKey
import sys, warnings, os
from collections import defaultdict

from utils.url_unshortener import bulkUnshorten

TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"



class TwitterRandomUserController(TwitterController):
    def make_random_integers(self, num_to_make=100):
        # valid ranges are in beginning,end tups
        VALID_RANGES = ((10008932, 3308208032),
                        (695135704930783232, 1095781403323707393))

        random_user_ids = []
        for i in range(num_to_make):
            rand_range_i = random.randrange(0, len(VALID_RANGES))
            #     print(rand_range_i)
            rand_range_low, rand_range_high = VALID_RANGES[rand_range_i]

            rand_user_int = random.randrange(rand_range_low, rand_range_high)
            random_user_ids.append(rand_user_int)
        random_user_ids = list(set(random_user_ids))  # to make sure we aren't sending duplicates to the API
        return random_user_ids

    def save_random_id_users(self, random_users_dict):
        """
        there should be a user id for every user that was guessed, if they existed they have a non-None user-detail dict.
        :param random_users_dict:
        :return:
        """
        twitter_users_to_add = []
        for user_id, user_details in random_users_dict.items():
            user_state = utils.common.TwitterUserState.FOUND.value \
                if user_details else utils.common.TwitterUserState.NOT_FOUND.value
            not_found_id = None if user_details else '{0}_{1}'.format(utils.common.NOT_FOUND_TWITTER_USER_STR, user_id)
            screen_name = user_details.screen_name if user_details else None
            created_at = datetime.datetime.strptime(user_details.created_at, utils.common.TWITTER_STRPTIME) \
                if user_details else None
            lang = user_details.status.lang if hasattr(user_details, 'status') and hasattr(user_details.status,'lang') else None
            last_status_dt = datetime.datetime.strptime(user_details.status.created_at, utils.common.TWITTER_STRPTIME) \
                if user_details and hasattr(user_details.status, 'created_at') \
                else None
            metadata_json = user_details._json if user_details else None
            rand_twitter_user = dict(
                id=user_id,
                not_found_id=not_found_id,
                user_state=user_state,
                screen_name=screen_name,
                created_at=created_at,
                lang=lang,
                last_status_dt=last_status_dt,
                metadata_json=metadata_json,
                created_type=utils.common.TwitterUserCreateType.RANDOMLY_GENERATED.value,
                CS_oldest_tweets_archived=utils.common.CS_JobState.NOT_PROCESSED.value
            )
            twitter_users_to_add.append(rand_twitter_user)

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", r"\(1062, \"Duplicate entry")
                self.db_session.execute(TwitterUser.__table__.insert().prefix_with("IGNORE"), twitter_users_to_add)
                self.db_session.commit()
        except sqlalchemy.exc.SQLAlchemyError:
            self.log.error("Error while saving random id twitter users for user ids: {}.".format(
                [u['id'] for u in twitter_users_to_add]), exc_info=True)

        return len(twitter_users_to_add)

    def num_random_id_generated_so_far_today(self):
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        today_guessed_q = self.db_session.query(TwitterUser).filter(
            TwitterUser.created_type == utils.common.TwitterUserCreateType.RANDOMLY_GENERATED). \
            filter(TwitterUser.record_created_at > yesterday)
        num_guessed_today = today_guessed_q.count()
        return num_guessed_today

    def generate_random_id_users(self, daily_limit=500000):
        '''
        https://github.com/SMAPPNYU/smapputil/blob/master/py/query_twitter/old_queries/generate_random_twitter_potential_ids.py
        from twitter :  These IDs are unique 64-bit unsigned integers, which are based on time, instead of being sequential. The full ID is composed of a timestamp, a worker number, and a sequence number.  When consuming the API using JSON, it is important to always use the field id_str instead of id.
        https://developer.twitter.com/en/docs/basics/twitter-ids
        Use a bloom filter to know what IDs have already been tried.
        Don't create more than daily limit number of users.
        '''
        # Get the number guessed today.
        if self.num_random_id_generated_so_far_today() >= daily_limit:
            return 0  # zero made in this batch

        random_user_ids = self.make_random_integers()
        # TODO use a bloom filter here
        # Get the IDs of those already guessed or Lumen-onboarded
        try:
            users_lookup_result = self.t.query(self.t.api.UsersLookup, user_id=random_user_ids)
        except TwitterError as e:
            if e.message[0]['code'] in (50, 63):
                pass
            else:
                raise e
        random_users_dict = {ruid: None for ruid in random_user_ids}
        for random_exist_user in users_lookup_result:
            random_users_dict[random_exist_user.id] = random_exist_user

        num_generated = self.save_random_id_users(random_users_dict)
        self.log.info("Persisted {num_generated} random ID users. {num_exist} actually existed.".format(
            num_generated=num_generated,
            num_exist=len(users_lookup_result)))
        return num_generated
