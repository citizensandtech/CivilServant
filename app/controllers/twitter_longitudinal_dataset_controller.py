import inspect
import math
import os
import random
import datetime
import csv
import time

import redis
from sqlalchemy import and_, func

from app.controllers.twitter_controller import TwitterController
from app.models import Base, TwitterUser, TwitterStatus, LumenNoticeToTwitterUser, TwitterUserSnapshot, TwitterFill, \
    TwitterUnshortenedUrls, TwitterStatusUrls, ExperimentThing, LumenNotice
import sqlalchemy
import utils.common
from utils.common import TwitterUserState, NOT_FOUND_TWITTER_USER_STR, CS_JobState, neq, EXPERIMENT_LANGUAGES, \
    TwitterUrlKey


class TwitterLongitudinalController(TwitterController):
    def __init__(self, db_session, twitter_conn, log, experiment_config, json_config):
        super().__init__(db_session, twitter_conn, log, experiment_config, json_config)
        self.now = datetime.datetime.utcnow()
        self.yesterday = self.now - datetime.timedelta(days=1)
        self.data_dir = self.json_config['data_dir']
        self.done_users = redis.Redis()
        self.rand_min = json_config['user_rand_frac_min']
        self.rand_max = json_config['user_rand_frac_max']
        print('computing users with rand between ({rand_min},{rand_max})')
        self.users_per_csv = json_config['users_per_output_csv'] if 'user_per_output_csv' in json_config.keys() else 100
        self.out_fields = ['twitter_user_id', 'twitter_user_created_at', 'notice_date', 'language', 'lumen_notice',
                           'date', 'day_num', 'num_lumen_notices', 'prev_lumen_notices', 'num_tweets', 'num_links',
                           'num_civic_links']

    def get_done_users(self):
        # TODO switch this to redis or make an output table, but going quickly for now
        csvs = [f for f in os.listdir(self.data_dir) if f.endswith('.csv')]
        done_users = set()
        for csv in csvs:
            with open(os.path.join(self.data_dir, csv), 'r') as duf:
                lg_reader = csv.reader(duf)
                for row in lg_reader:
                    # assuming the twitter_user_id is in column 0
                    done_users.add(row[0])
        return done_users

    def to_day(self, date):
        return datetime.date(date.year, date.month, date.day)

    def start_user_records_batch(self, batch_id):
        batch_csv_f = os.path.join(self.data_dir, 'min_{}_max_{}_part_{}'.format(self.rand_min, self.rand_max, batch_id))
        self.batch_csv = open(batch_csv_f, 'a')
        self.batch_csv_writer = csv.DictWriter(self.batch_csv, fieldnames=self.out_fields)
        self.batch_csv_writer.writeheader()

    def close_user_records_batch(self, batch_id):
        self.batch_csv.close()

    def write_user_records(self, user_records):
        for user_record in user_records:
            self.batch_csv_writer.writerow(user_record)



    def process_users_batch(self, users, batch_id):
        done_users = self.get_done_users()
        now_time = time.time()
        self.start_user_records_batch(batch_id)
        for i, user in enumerate(users):
            if user.id in done_users:
                print('already did: {}'.format(user.id))
                continue
            if i % 20 == 0:
                print("...in process_users: completed {0}/{1} users".format(i, len(users)))
                print("last user took {} secs".format(time.time() - now_time))
            now_time = time.time()
            user_records = self.process_user(user)
            self.write_user_records(user_records)
        self.close_user_records_batch()

    def process_user(self, user):
        (user_id, user_created_at, language) = (user[0], user[1], user[2])
        # (notice id, notice date received)
        # TODO switch this to twitter_statuses_recent, may require manual SQL
        lumen_notice_dates = self.db_session \
            .query(LumenNoticeToTwitterUser.notice_id, LumenNotice.date_received) \
            .join(LumenNotice, LumenNoticeToTwitterUser.notice_id == LumenNotice.id) \
            .filter(LumenNoticeToTwitterUser.twitter_user_id == user_id) \
            .all()

        lumen_notices_by_day = {}
        for l in lumen_notice_dates:
            this_day = self.to_day(l.date_received)
            if this_day not in lumen_notices_by_day:
                lumen_notices_by_day[this_day] = set([])
            lumen_notices_by_day[this_day].add(l)

        # (status id, status created at, status expanded url, status unshortened url, domain, status tld, status tld is civic)
        # essentially: 1st filter by tld matching, 2nd filter by checking that expanded/unshortened_url is a subdomain of normalized domain
        # TODO: if you want to identify media only vs text only tweets, you'd query for the tweet json here
        tweet_urls = self.db_session \
            .query(TwitterStatus.id.label("status_id"), TwitterStatus.created_at,
                   TwitterStatusUrls.id.label("status_url_id"), TwitterStatusUrls.expanded_url,
                   TwitterStatusUrls.unshortened_url,) \
            .filter(TwitterStatus.user_id == user_id) \
            .all()

        tweet_just_urls_by_day = {}  # counting unique (tweet id, url)s where url is not null
        tweet_urls_by_day = {}  # (tweet id, url, whitelist info)s for determining is_civic, where url is not null
        tweets_by_day = {}  # counting unique tweet ids
        for t in tweet_urls:
            this_day = self.to_day(t.created_at)
            if t.expanded_url or t.unshortened_url:
                # if has url
                if this_day not in tweet_urls_by_day:
                    tweet_urls_by_day[this_day] = set([])
                tweet_urls_by_day[this_day].add(t)
                if this_day not in tweet_just_urls_by_day:
                    tweet_just_urls_by_day[this_day] = set([])
                tweet_just_urls_by_day[this_day].add((t.status_id, t.status_url_id))

            if this_day not in tweets_by_day:
                tweets_by_day[this_day] = set([])
            tweets_by_day[this_day].add(t.status_id)
        sorted_notices = sorted(lumen_notice_dates, key=lambda e: e[1])

        (earliest_notice, earliest_date) = sorted_notices[0]
        earliest_day = self.to_day(earliest_date)

        prev_notices = 0
        records = []
        for day_offset in range(-23, 24):
            today = earliest_day + datetime.timedelta(days=day_offset)
            # print("{2}: day_offset = {0}, today = {1}".format(day_offset, today, user_id))

            num_notices = len(lumen_notices_by_day[today]) if today in lumen_notices_by_day else 0
            prev_notices += num_notices
            num_tweets = len(tweets_by_day[today]) if today in tweets_by_day else 0
            num_links = len(tweet_just_urls_by_day[today]) if today in tweet_just_urls_by_day else 0
            num_civic_links = self.get_num_civic_links(tweet_urls_by_day[today]) if today in tweet_urls_by_day else 0
            record = dict(
                user_id=user_id,
                user_created_at=user_created_at,
                earliest_date=earliest_date,
                language=language,
                earliest_notice=earliest_notice,
                today=today,
                day_offset=day_offset,
                num_notices=num_notices,
                prev_notices=prev_notices,
                num_tweets=num_tweets,
                num_links=num_links,
                num_civic_links=num_civic_links,)
            records.append(record)
        return records

    def get_num_civil_links(self, tweet_urls):
        # TODO implement
        return None
    def run(self):
        # TODO: if you want to include info from user json, you can include that in this query
        # e.g. other user info: created_at, default_profile_image, default_name_field,
        # default_account_description, default_account_website
        user_incl_cond = and_(TwitterUser.user_rand < self.rand_max,
                              TwitterUser.user_rand > self.rand_min,
                              TwitterUser.lang.in_(("en", "en-gb", "en-GB")))

        users = self.db_session \
            .query(TwitterUser.id, TwitterUser.created_at, TwitterUser.lang) \
            .filter(user_incl_cond)

        count = self.db_session \
            .query(func.count(TwitterUser.id)) \
            .filter(user_incl_cond) \
            .first()[0]

        print("Processing {0} twitter users".format(count))

        for i in range(0, count, self.users_per_csv):
            print("..completed users count = ", i)
            batch_users = users[i:i + self.users_per_csv]

            self.process_users_batch(users=batch_users, batch_id=i)

        print("writing complete")
