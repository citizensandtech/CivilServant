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
        to save on space, only saving existing users.
        :param random_users_dict:
        :return:
        """
        twitter_users_to_add = []
        # subest to just users who had responses
        existing_users = {user_id: user_details for user_id, user_details in random_users_dict.items() if user_details}
        for user_id, user_details in existing_users.items():
            user_state = utils.common.TwitterUserState.FOUND.value
            not_found_id = None
            screen_name = user_details.screen_name
            created_at = datetime.datetime.strptime(user_details.created_at, utils.common.TWITTER_STRPTIME)
            lang = user_details.status.lang if hasattr(user_details, 'status') \
                                               and hasattr(user_details.status, 'lang') \
                                           else None
            last_status_dt = datetime.datetime.strptime(user_details.status.created_at,
                                                        utils.common.TWITTER_STRPTIME) \
                                                if hasattr(user_details.status, 'created_at') \
                                            else None
            metadata_json = user_details._json

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
            self.db_session.insert_retryable(TwitterUser, twitter_users_to_add)
        except sqlalchemy.exc.SQLAlchemyError as e:
            self.log.error("Error {} while saving random id twitter users for user ids: {}.".format(
                e, [u['id'] for u in twitter_users_to_add]), exc_info=True)

        return len(twitter_users_to_add)

    def num_random_id_generated_so_far_today(self):
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        today_guessed_existed_q = self.db_session.query(TwitterUser).filter(
            TwitterUser.created_type == utils.common.TwitterUserCreateType.RANDOMLY_GENERATED.value) \
            .filter(TwitterUser.user_state == utils.common.TwitterUserState.FOUND.value) \
            .filter(TwitterUser.record_created_at > yesterday)
        num_guessed_existed_today = today_guessed_existed_q.count()
        return num_guessed_existed_today

    def generate_random_id_users(self, daily_limit=500000, target_additions=500):
        '''
        https://github.com/SMAPPNYU/smapputil/blob/master/py/query_twitter/old_queries/generate_random_twitter_potential_ids.py
        from twitter :  These IDs are unique 64-bit unsigned integers, which are based on time, instead of being sequential. The full ID is composed of a timestamp, a worker number, and a sequence number.  When consuming the API using JSON, it is important to always use the field id_str instead of id.
        https://developer.twitter.com/en/docs/basics/twitter-ids
        Use a bloom filter to know what IDs have already been tried.
        Don't create more than daily limit number of users.
        num_additions is the number of valid id users to target adding.
        '''
        num_exist = 0
        num_generated = 0
        # Get the number guessed today.
        if self.num_random_id_generated_so_far_today() >= daily_limit:
            return num_exist  # zero made in this batch
        else:
            round = 0
            while num_exist < target_additions:
                self.log.debug("Getting random users in rounds. round is {round}".format(round=round))
                num_exist_round, num_generated_round = self._generate_single_round_of_random_users()
                num_exist += num_exist_round
                num_generated += num_generated_round
                round += 1
        self.log.info("New existing users target met...."
                      "Persisted {num_generated} random ID users. {num_exist} actually existed."
                      "Proportion existing={prop}".format(
            num_generated=num_generated, num_exist=num_exist, prop='{0:.2f}'.format(num_exist / num_generated)))
        return num_exist

    def _generate_single_round_of_random_users(self):
        # twitter api doesn't allow more than 100 ids per query.
        random_user_ids = self.make_random_integers(100)
        # TODO use a bloom filter here
        # Get the IDs of those already guessed or Lumen-onboarded
        try:
            users_lookup_result = self.t.query(self.t.api.UsersLookup, user_id=random_user_ids)
        except TwitterError as e:
            self.log.error(e)
            if e.message[0]['code'] in (50, 63):
                pass
            else:
                raise e
        random_users_dict = {ruid: None for ruid in random_user_ids}
        for random_exist_user in users_lookup_result:
            random_users_dict[random_exist_user.id] = random_exist_user

        num_generated_round = self.save_random_id_users(random_users_dict)
        num_exist_round = len(users_lookup_result)
        self.log.info("Persisted {num_generated} random ID users. {num_exist} actually existed.".format(
            num_generated=num_generated_round,
            num_exist=num_exist_round))
        return num_exist_round, num_generated_round
