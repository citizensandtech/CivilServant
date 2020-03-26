import math
import random
from operator import eq, ne

import redis
import twitter
import simplejson as json
import datetime

from twitter import TwitterError

from app.controllers.twitter_controller import TwitterController
from app.models import Base, TwitterUser, TwitterStatus, LumenNoticeToTwitterUser, TwitterUserSnapshot, TwitterFill, \
    TwitterUnshortenedUrls, TwitterStatusUrls, ExperimentThing
import requests
import sqlalchemy
from sqlalchemy import and_, or_, func, distinct
import utils.common
from utils.common import TwitterUserState, NOT_FOUND_TWITTER_USER_STR, CS_JobState, neq, EXPERIMENT_LANGUAGES, \
    TwitterUrlKey
import sys, warnings, os
from collections import defaultdict

from utils.url_unshortener import bulkUnshorten

TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"



class TwitterUnshortenController(TwitterController):
    def unshorten_urls(self, unshorten_batch_size=1000, idempotent=True):
        # iterate over twitter_status_urls converting expanded urls to unshortened urls
        # get the max and minimum status ids
        # batch between those # 10,000 items
        # run the url unshortener on the batch
        # re-insert the results based on table id or expanded_url
        status_url_id_max = self.db_session.query(func.max(TwitterStatusUrls.id)).one()[0]
        status_url_id_min = self.db_session.query(func.min(TwitterStatusUrls.id)).one()[0]
        status_url_id_cnt = self.db_session.query(func.count(TwitterStatusUrls.id)).one()[0]
        self.log.info('status_url_id_max is {status_url_id_max}'.format(status_url_id_max=status_url_id_max))
        self.log.info('status_url_id_min is {status_url_id_min}'.format(status_url_id_min=status_url_id_min))
        self.log.info('status_url_id_cnt is {status_url_id_cnt}'.format(status_url_id_cnt=status_url_id_cnt))

        num_batches = math.ceil((status_url_id_max - status_url_id_min) / unshorten_batch_size)
        for batch_i in range(num_batches):
            start_id = status_url_id_min + (batch_i * unshorten_batch_size)
            end_id = status_url_id_min + ((batch_i + 1) * unshorten_batch_size)
            self.log.debug('working on status url ids {start_id} --- {end_id}'.format(start_id=start_id, end_id=end_id))
            batch_status_urls_a = self.db_session.query(TwitterStatusUrls) \
                .filter(and_(TwitterStatusUrls.id >= start_id, TwitterStatusUrls.id < end_id)).all()

            if idempotent:
                batch_status_urls = [su for su in batch_status_urls_a if
                                     (su.unshortened_url is None and su.error_unshortening is None)]
            else:
                batch_status_urls = batch_status_urls_a

            self.log.info('Working on batch:{batch_i} {len_batch_status_urls} status urls'.format(batch_i=batch_i,
                                                                                                  len_batch_status_urls=len(
                                                                                                      batch_status_urls)))
            urls_to_unshorten = [su.expanded_url for su in batch_status_urls]

            if urls_to_unshorten:
                # run them through the unshortener
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    unshort_results = bulkUnshorten(urls_to_unshorten)

                # stich these back up
                for unshort_res in unshort_results:
                    # find the db objects associated
                    matching_sus = [su for su in batch_status_urls if unshort_res['original_url'] == su.expanded_url]
                    for matching_su in matching_sus:
                        matching_su.unshortened_url = unshort_res['final_url']
                        matching_su.error_unshortening = unshort_res['error'] if not unshort_res['success'] else None

                self.db_session.add_all(batch_status_urls)
                self.db_session.commit()

    def output_unshorten_urls(self):
        # deprecated based on new way unshortening is happening
        r = redis.Redis()

        status_users_res = self.db_session.query(distinct(TwitterStatus.user_id)).all()
        status_user_ids = [user_tup[0] for user_tup in status_users_res if user_tup[0]]

        for i, status_user_id in enumerate(status_user_ids):
            self.log.info(
                'Unshortening URLS for user id {0}. {1} of {2}'.format(status_user_id, i, len(status_user_ids)))
            user_statuses = self.db_session.query(TwitterStatus).filter(TwitterStatus.user_id == status_user_id).all()
            status_urls_flat = []

    def extract_urls(self, twitter_uid=None):

        if twitter_uid is None:
            status_users_res = self.db_session.query(distinct(TwitterStatus.user_id)).all()
            status_user_ids = [user_tup[0] for user_tup in status_users_res if user_tup[0]]
        else:
            status_user_ids = [int(twitter_uid)]

        for i, status_user_id in enumerate(status_user_ids):
            self.log.info('Extracting URLS for user id {0}. {1} of {2}'.format(status_user_id, i, len(status_user_ids)))
            # get all user's tweets

            user_statuses = self.db_session.query(TwitterStatus).filter(TwitterStatus.user_id == status_user_id).all()

            for user_status in user_statuses:
                status_data = json.loads(user_status.status_data)
                url_rows = self.extract_urls_from_status_data(user_status.id, status_data, None)
                self.db_session.add_all(url_rows)
                self.db_session.commit()

    # returns list of TwitterStatusUrls
    def extract_urls_from_status_data(self, status_id, status_data, default_key):
        url_rows = []

        if 'entities' in status_data and 'urls' in status_data['entities']:
            urls = status_data['entities']['urls']
            key = default_key if default_key is not None else TwitterUrlKey.ENTITY

            for url in urls:
                url_row = TwitterStatusUrls(
                    twitter_status_id=status_id,
                    status_data_key=key.value,
                    raw_url=url['url'] if 'url' in url else None,
                    expanded_url=url['expanded_url'] if 'expanded_url' in url else None,
                    unwound_url=url['unwound']['url'] if 'unwound' in url and 'url' in data['unwound'] else None)

                # self.log.info('...status id {0}: got url={1}; total={2}'.format(status_id, url['url'], len(url_rows)+1))
                url_rows.append(url_row)

        if 'extended_entities' in status_data and 'media' in status_data['extended_entities']:
            urls = status_data['extended_entities']['media']
            key = default_key if default_key is not None else TwitterUrlKey.EXTENDED
            if key is TwitterUrlKey.RETWEETED_ENTITY:
                key = TwitterUrlKey.RETWEETED_EXTENDED
            elif key is TwitterUrlKey.QUOTED_ENTITY:
                key = TwitterUrlKey.QUOTED_EXTENDED

            for media in urls:
                url_row = TwitterStatusUrls(
                    twitter_status_id=status_id,
                    status_data_key=key.value,
                    raw_url=media['url'] if 'url' in media else None,
                    expanded_url=media['expanded_url'] if 'expanded_url' in media else None,
                    unwound_url=media['unwound']['url'] if 'unwound' in media and 'url' in media['unwound'] else None)

                # self.log.info('...user id {0}: got url; total={1}'.format(status_id, len(url_rows)+1))
                url_rows.append(url_row)

        if 'retweeted_status' in status_data:
            retweeted_url_rows = self.extract_urls_from_status_data(status_id,
                                                                    status_data['retweeted_status'],
                                                                    TwitterUrlKey.RETWEETED_ENTITY)
            url_rows += retweeted_url_rows

        if 'quoted_status' in status_data:
            quoted_url_rows = self.extract_urls_from_status_data(status_id,
                                                                 status_data['quoted_status'],
                                                                 TwitterUrlKey.QUOTED_ENTITY)
            url_rows += quoted_url_rows

        return url_rows
