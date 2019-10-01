import random
from operator import eq

import redis
import twitter
import simplejson as json
import datetime
from app.models import Base, TwitterUser, TwitterStatus, LumenNoticeToTwitterUser, TwitterUserSnapshot, TwitterFill, \
    TwitterUnshortenedUrls, TwitterStatusUrls, TwitterTlds
import requests
import sqlalchemy
from sqlalchemy import and_, or_, func, distinct
import utils.common
from utils.common import TwitterUserState, NOT_FOUND_TWITTER_USER_STR, CS_JobState, neq, EXPERIMENT_LANGUAGES, TwitterUrlKey
import sys, warnings, os
from collections import defaultdict

from utils.url_unshortener import bulkUnshorten

import tldextract

TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"


class TwitterAnalysisController():
    def __init__(self, db_session, db_session_write, t, log):
        self.t = t
        self.db_session = db_session
        self.db_session_write = db_session_write
        self.log = log

    # "script 2"
    def get_tlds(self, start, end):
        batch = 5000
        for i in range(start, end):
            rows = self.db_session.query(TwitterStatusUrls) \
                .filter(and_(TwitterStatusUrls.id >= i*batch, \
                        TwitterStatusUrls.id < (i+1)*batch, \
                        TwitterStatusUrls.tld_text == None \
                        ))
            self.log.info('Extracting tlds from URLS... {0} rows'.format(i*batch))
            for row in rows:
                full_url = row.unshortened_url if row.unshortened_url is not None else row.expanded_url
                tld = tldextract.extract(full_url).registered_domain.lower() # primary key
                self.log.info('...... id={0}, url={1}, tld={2}'.format(row.id, full_url, tld))
                row.tld_text = tld
                self.db_session.commit()

    # "script 1"
    def extract_urls2(self):
        i = 0
        count = 0
        for user_status in self.db_session.query(TwitterStatus).order_by(TwitterStatus.id).yield_per(1000):
            self.log.info('Extracting URLS... {0} statuses, {1} urls; current status id = {2}'.format(i, count, user_status.id))
            status_data = json.loads(user_status.status_data.encode("utf-8", "replace")) if user_status.status_data is not None else None
            url_rows = self.extract_urls_from_status_data(user_status.id, status_data, None)
            i += 1
            count += len(url_rows)
            if len(url_rows) > 0:
                self.log.info("...status id {0} attempting commit of {1} urls".format(user_status.id, len(url_rows)))
                self.db_session_write.add_all(url_rows)
                self.db_session_write.commit()
                self.log.info("...status id {0} commited {1}".format(user_status.id, len(url_rows)))
            else:
                self.db_session_write.flush()


    def extract_urls(self, twitter_uid=None):
 
        if twitter_uid is None:
            status_users_res = self.db_session.query(distinct(TwitterStatus.user_id)).all()
            status_user_ids = [user_tup[0] for user_tup in status_users_res if user_tup[0]]
        else:
            status_user_ids = [int(twitter_uid)]

        for i, status_user_id in enumerate(status_user_ids):
            self.log.info('Extracting URLS for user id {0}. {1} of {2}'.format(status_user_id, i, len(status_user_ids)))
            # get all user's tweets

            user_statuses = self.db_session.query(TwitterStatus).filter(TwitterStatus.user_id==status_user_id).all()

            for user_status in user_statuses:
                status_data = json.loads(user_status.status_data)
                url_rows = self.extract_urls_from_status_data(user_status.id, status_data, None)
                self.db_session.add_all(url_rows)
                self.db_session.commit()


    # returns list of TwitterStatusUrls
    def extract_urls_from_status_data(self, status_id, status_data, default_key):
        url_rows = []
        if status_data is None:
            return url_rows

        if 'entities' in status_data and 'urls' in status_data['entities']:
            urls = status_data['entities']['urls']
            key = default_key if default_key is not None else TwitterUrlKey.ENTITY

            for url in urls:
                url_row = TwitterStatusUrls(
                    twitter_status_id = status_id,
                    status_data_key = key.value,
                    raw_url = url['url'] if 'url' in url else None,
                    expanded_url = url['expanded_url'] if 'expanded_url' in url else None,
                    unwound_url = url['unwound']['url'] if 'unwound' in url and 'url' in data['unwound'] else None)

                self.log.info('...status id {0}: got url={1}; total={2}'.format(status_id, url['url'], len(url_rows)+1))
                print(">>>>>> url_row.expanded_url: {0}".format(url_row.expanded_url))
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
                    twitter_status_id = status_id,
                    status_data_key = key.value, 
                    raw_url = media['url'] if 'url' in media else None,
                    expanded_url = media['expanded_url'] if 'expanded_url' in media else None,
                    unwound_url = media['unwound']['url'] if 'unwound' in media and 'url' in media['unwound'] else None)

                self.log.info('...status id {0}: got url={1}; total={2}'.format(status_id, media['url'], len(url_rows)+1))
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
