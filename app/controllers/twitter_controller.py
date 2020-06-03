import math
import random
from operator import eq, ne

import redis
import twitter
import simplejson as json
import datetime

from twitter import TwitterError

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

"""

Some notes about twitter users:

if in a LumenNoticeToTwitterUser record
    twitter_username = utils.common.NOT_FOUND_TWITTER_USER_STR,
    twitter_user_id = utils.common.NOT_FOUND_TWITTER_USER_STR,

    then
        the account parsed from the link (a t.co link) now suspended.
        we don't know the username or the userid, and never will.
        we don't know if there are users mentioned more than once
        so we do not store TwitterUser records for these users

if in a TwitterUser record
    id = [actual twitter id]
    not_found_id = None
    screen_name = name

    then
        we had parsed a username from a link
        and we found the user info
        and the user has never been "not found" (NOT_FOUND or SUSPENDED)

if in a TwitterUser record
    id = utils.common.generate_not_found_twitter_user_id(name)
    not_found_id = utils.common.generate_not_found_twitter_user_id(name)
    screen_name = name

    then
        we had parsed a username from a link
        but we have never found the user info
        the user has always been seen to be NOT_FOUND or SUSPENDED

if in a TwitterUser record
    id = [actualy twitter id]
    not_found_id = utils.common.generate_not_found_twitter_user_id(name)
    screen_name = name

    then
        we had parsed a username from a link
        at some point the user has also been FOUND or PROTECTED, since we found the user info
        at some point the user has been NOT_FOUND or SUSPENDED (since we once assigned a not_found_id to them),

Note that if a username changes for an account that we don't have the id for, we will have no idea.

"""


class TwitterController():
    def __init__(self, db_session, t, log, config=None):
        self.t = t
        self.db_session = db_session
        self.log = log
        self.config = config

    #########################################################
    ################### ARCHIVE NEW USERS CODE
    #########################################################

    def query_and_archive_new_users(self, test_exception=False):
        # get unprocessed LumenNoticeToTwitterUser records with real twitter usernames
        unarchived_notice_users = self.db_session.query(LumenNoticeToTwitterUser).filter(
            # or_(
            LumenNoticeToTwitterUser.CS_account_archived == CS_JobState.NOT_PROCESSED.value  # ,
            # LumenNoticeToTwitterUser.CS_account_archived == CS_JobState.IN_PROGRESS.value)
        ).filter(
            LumenNoticeToTwitterUser.twitter_username != utils.common.NOT_FOUND_TWITTER_USER_STR
            # you may get these from t.co links
        ).all()

        unarchived_names = [nu.twitter_username.lower() for nu in unarchived_notice_users]

        # process all usernames that DON'T have TwitterUser records OR have records but don't have real ids
        existing_users = []
        if len(unarchived_names) > 0:
            existing_users = self.db_session.query(TwitterUser).filter(
                TwitterUser.screen_name.in_(list(unarchived_names))).filter(
            ).all()

        existing_usernames = set(
            [e.screen_name.lower() for e in existing_users if utils.common.NOT_FOUND_TWITTER_USER_STR not in e.id])

        processed_unarchived_notice_users = [nu for nu in unarchived_notice_users if
                                             nu.twitter_username.lower() in existing_usernames]
        utils.common.update_CS_JobState(processed_unarchived_notice_users, "CS_account_archived", CS_JobState.PROCESSED,
                                        self.db_session, self.log)

        unprocessed_unarchived_notice_users = [nu for nu in unarchived_notice_users if
                                               nu.twitter_username.lower() not in existing_usernames]
        utils.common.update_CS_JobState(unprocessed_unarchived_notice_users, "CS_account_archived",
                                        CS_JobState.IN_PROGRESS, self.db_session, self.log)

        try:
            self.archive_new_users(unprocessed_unarchived_notice_users, test_exception)
        except:
            raise  # re-raise the exception
        finally:
            # reset progress for any remaining in-progress items whether or not exception is raised
            notice_users_to_reset = [notice_user for notice_user in unprocessed_unarchived_notice_users if
                                     notice_user.CS_account_archived == CS_JobState.IN_PROGRESS.value]
            utils.common.reset_CS_JobState_In_Progress(notice_users_to_reset, "CS_account_archived", self.db_session,
                                                       self.log)  # if still marked IN_PROGRESS (e.g. because of unchecked exception), reset it to NOT_PROCESSED

    def archive_new_users(self, unarchived_notice_users, test_exception=False):
        """
            unarchived_notice_users: list of LumenNoticeToTwitterUser

            archive_new_users makes sure that new users get a TwitterUser and TwitterUserSnapshot stored for them
            currently, it is NOT responsible for updating existing TwitterUser objects
            (archive_old_users is responsible for that, e.g. in the case that a user goes from found to not found)
            however, it should make sure that it doesn't add duplicate entries for the same user


            for username in unarchived_notice_users:
                if user id found:
                    if user id not already stored:
                        store new TwitterUser, TwitterUserSnapshot
                    else:
                        do nothing
                elif no user id (or user info) found:
                    if username not already stored (gives approx correct behavior):
                        store new TwitterUser, TwitterUserSnapshot
                    else:
                        do nothing


        """
        if len(unarchived_notice_users) == 0:
            return (None, None)

        if len(unarchived_notice_users) <= 0:
            return (None, None)

        # JNM NOTE TODO: SHOULD PROBABLY BE A DICT OF LISTS, NOT A DICT OF OBJECTS
        # HERE WE ASSUME THAT ACCOUNTS THAT HAVE THE SAME SCREEN NAME
        # AND WHICH ARE IN THIS SCRAPING SESSION ARE THE SAME ACCOUNT
        user_names_to_notice_user = defaultdict(list)
        for nu in unarchived_notice_users:
            if (utils.common.NOT_FOUND_TWITTER_USER_STR not in nu.twitter_username):
                user_names_to_notice_user[nu.twitter_username].append(nu)
        #       user_names_to_notice_user = {nu.twitter_username: nu for nu in unarchived_notice_users if utils.common.NOT_FOUND_TWITTER_USER_STR not in nu.twitter_username}
        unarchived_user_names = set(user_names_to_notice_user.keys())
        user_names = list(unarchived_user_names)

        # query batch_size at a time
        batch_size = 100  # limit should be 100
        prev_limit = 0
        left_users = unarchived_user_names  # reference

        all_existing_ids = set([])  # all ids already stored in db

        def commit_users_failed(user):
            for noticeuser in user_names_to_notice_user[user]:
                noticeuser.CS_account_archived = CS_JobState.FAILED.value
                # noticeuser.twitter_user_id = user_name_to_id[noticeuser.twitter_username]
                self.db_session.add(noticeuser)
            self.db_session.commit()

        if test_exception:
            counter = 0
        for i in range(1, int(len(user_names) / batch_size) + 2):
            limit = min(i * batch_size, len(user_names))
            if limit > prev_limit:
                # query twitter API for user info
                this_users = user_names[prev_limit:limit]
                users_info = []
                try:
                    users_info = self.t.query(self.t.api.UsersLookup, screen_name=this_users)
                except twitter.error.TwitterError as e:
                    # self.t.try_counter = 0 ## this line prevents the retry code from looping
                    # for this_user in this_users:
                    #    commit_users_failed(this_user)
                    self.log.info("Failed to query for {0} Twitter users using api.UsersLookup: {1} {2}".format(
                        limit - prev_limit, ",".join(this_users), str(e)))
                else:
                    self.log.info(
                        "Queried for {0} Twitter users out of a total of {1} users, got {2} out of {3} users".format(
                            limit, len(user_names), len(users_info), limit - prev_limit))
                prev_limit = limit

                # for found users, commit to db

                users_json = [json.loads(json.dumps(user_info._json).encode("utf-8", "replace")) if type(
                    user_info) is twitter.models.User else user_info for user_info in
                              users_info]  # to accomodate test fixture data

                this_found_ids = set([user_json["id"] for user_json in users_json])
                existing_ids = [uid[0] for uid in self.db_session.query(TwitterUser.id).filter(
                    TwitterUser.id.in_(list(this_found_ids))).all()]
                all_existing_ids.update(existing_ids)

                for user_json in users_json:
                    uid = str(user_json["id"])
                    screen_name = user_json["screen_name"].lower()

                    if uid not in all_existing_ids and screen_name in left_users:
                        # if uid not in all_existing_ids: if this id hasn't been seen before. need to do this if querying off usernames, since usernames can change.
                        # if (uid in left_users or screen_name in left_users): then we haven't seen this screen_name before. else, don't archive. actually this is a redundant check
                        created_at = datetime.datetime.strptime(user_json["created_at"], utils.common.TWITTER_STRPTIME)

                        # determine user state
                        user_state = TwitterUserState.FOUND if not user_json[
                            "protected"] else TwitterUserState.PROTECTED

                        now = datetime.datetime.utcnow()
                        try:
                            # create TwitterUser record
                            user_record = TwitterUser(
                                id=uid,
                                not_found_id=None,
                                screen_name=screen_name,  # usernames change! index/search on id when possible.
                                created_at=created_at,
                                # is UTC; expected string format: "Mon Nov 29 21:18:15 +0000 2010"
                                created_type=utils.common.TwitterUserCreateType.LUMEN_NOTICE.value,
                                record_created_at=now,
                                lang=user_json["status"]["lang"] if 'status' in user_json.keys() and 'lang' in user_json['status'].keys() else None,
                                last_status_dt=datetime.datetime.strptime(user_json["status"]["created_at"], utils.common.TWITTER_STRPTIME) if 'status' in user_json.keys() and 'created_at' in user_json['status'].keys() else None,
                                metadata_json=user_json if user_json else None,
                                user_state=user_state.value,
                                CS_oldest_tweets_archived=CS_JobState.NOT_PROCESSED.value)
                            self.db_session.add(user_record)

                            # create first TwitterUserSnapshot record
                            user_snapshot_record = TwitterUserSnapshot(
                                twitter_user_id=uid,
                                twitter_not_found_id=None,
                                record_created_at=now,
                                user_state=user_state.value,
                                user_json=json.dumps(user_json))  # already encoded
                            self.db_session.add(user_snapshot_record)

                            all_existing_ids.add(uid)
                            left_users.discard(screen_name)  # discard doesn't throw an error

                            ## iterate through the list of notice users and
                            ## update all of the records
                            noticeusers = user_names_to_notice_user[screen_name]
                            for nu in noticeusers:
                                nu.CS_account_archived = CS_JobState.PROCESSED.value
                                nu.twitter_user_id = uid
                                self.db_session.add(nu)

                            if test_exception:
                                counter += 1
                                if counter >= 2:
                                    raise Exception("Throwing an exception for test purposes")

                            try:
                                self.db_session.commit()
                            except:
                                self.log.error(
                                    "Error while saving DB Session for TwitterUser, TwitterUserSnapshot object",
                                    exc_info=True)
                                commit_users_failed(screen_name)
                        except:
                            self.log.error(
                                "Error while creating TwitterUser, TwitterUserSnapshot objects for user {0}".format(
                                    user_json["id"]), exc_info=True)
                            commit_users_failed(screen_name)

        # at end, for left_users (users not found), commit to db
        left_existing_users = self.db_session.query(TwitterUser).filter(
            TwitterUser.screen_name.in_(list(left_users))).all()

        # first, update lumen notices that are already associated with a twitter user
        for twitter_user in left_existing_users:
            screen_name = twitter_user.screen_name
            if screen_name in user_names_to_notice_user.keys():
                for notice_user in user_names_to_notice_user[screen_name]:
                    if (notice_user.CS_account_archived == CS_JobState.IN_PROGRESS.value):
                        notice_user.twitter_user_id = twitter_user.id
                        notice_user.CS_account_archived = CS_JobState.PROCESSED.value
                        self.db_session.add(notice_user)
        self.db_session.commit()

        # next, remove left_users that already have a TwitterUser record (with the same screen_name)
        left_users = left_users - set([u.screen_name for u in left_existing_users])

        for name in left_users:
            uid = utils.common.generate_not_found_twitter_user_id(name)

            # disambiguate between NOT_FOUND, SUSPENDED
            user_state = self.is_user_suspended_or_deleted(name)

            now = datetime.datetime.utcnow()
            try:
                # create TwitterUser record
                user_record = TwitterUser(
                    id=uid,
                    not_found_id=uid,
                    screen_name=name,
                    created_at=None,
                    record_created_at=now,
                    lang=None,
                    user_state=user_state.value,
                    CS_oldest_tweets_archived=CS_JobState.WONT_PROCESS.value)  # no tweets to find
                # if we set these NOT_FOUND or SUSPENDED users to CS_JobState.PROCESSED then they might be tried again
                # but we aren't interseted
                self.db_session.add(user_record)

                # also create first TwitterUserSnapshot record
                user_snapshot_record = TwitterUserSnapshot(
                    twitter_user_id=uid,
                    twitter_not_found_id=uid,
                    record_created_at=now,
                    user_state=user_state.value,
                    user_json=None)
                self.db_session.add(user_snapshot_record)

                noticeusers = user_names_to_notice_user[name]
                for noticeuser in noticeusers:
                    noticeuser.CS_account_archived = CS_JobState.PROCESSED.value
                    noticeuser.twitter_user_id = uid
                    self.db_session.add(noticeuser)

                try:
                    self.db_session.commit()
                except:
                    self.log.error("Error while saving DB Session for TwitterUser, TwitterUserSnapshot object",
                                   exc_info=True)
                    commit_users_failed([name])
            except:
                self.log.error(
                    "Error while updating TwitterUser, creating TwitterUserSnapshot object for user {0}".format(
                        user_json["id"]), exc_info=True)
                commit_users_failed([name])

    def is_user_suspended_or_deleted(self, username):
        user_state = TwitterUserState.NOT_FOUND
        try:
            user = self.t.query(self.t.api.GetUser, screen_name=username)
            user_state = TwitterUserState.FOUND
        except twitter.error.TwitterError as e:
            self.t.try_counter = 0  ## this line prevents the retry code from looping
            if e.message[0]['code'] == 50 and e.message[0]['message'] == 'User not found.':
                user_state = TwitterUserState.NOT_FOUND
            elif e.message[0]['code'] == 63 and e.message[0]['message'] == 'User has been suspended.':
                user_state = TwitterUserState.SUSPENDED
            else:
                self.log.error(
                    "Unexpected twitter.error.TwitterError exception while calling api.GetUser on user {0}: {1}".format(
                        username, e))
        return user_state

    #########################################################
    ################### ARCHIVE SNAPSHOTS AND NEW TWEETS CODE
    #########################################################

    def query_and_archive_user_snapshots_and_tweets(self, min_time, is_test=False):
        """
            precondition: a TwitterUser and TwitterUserSnapshot must exist for
                            the user, for archive_old_user to run

            for each user in twitterusersnapshot with too old most recent snapshot:
                user_state twitterusersnapshot record
                update twitteruser?
                store tweets?

            doesn't need to update any CS_JobState fields.
        """

        need_snapshot_users = self.db_session.query(TwitterUser).filter(
            or_(TwitterUser.lang.in_(EXPERIMENT_LANGUAGES), TwitterUser.lang is None)).all()

        # querying TwitterUserSnapshot is very expensive
        # need_snapshot_user_snapshots = self.db_session.query(
        #    TwitterUserSnapshot.twitter_user_id).group_by(
        #    TwitterUserSnapshot.twitter_user_id).having(
        #    func.max(TwitterUserSnapshot.record_created_at) < min_time).all()

        # make sure to get unique ids
        need_snapshot_user_ids = [u.id for u in need_snapshot_users]
        self.log.info("Need to update snapshots for {0} users".format(len(need_snapshot_user_ids)))
        if len(need_snapshot_user_ids) <= 0:
            return

        # store TwitterUserSnapshot, update TwitterUser for all queried users
        need_snapshot_id_to_all_user = {u.id: u for u in need_snapshot_users}
        need_snapshot_id_to_found_user = {uid: need_snapshot_id_to_all_user[uid] for uid in need_snapshot_id_to_all_user
                                          if utils.common.NOT_FOUND_TWITTER_USER_STR not in uid}
        self.archive_old_users(need_snapshot_id_to_found_user, has_ids=True)
        need_snapshot_names_to_not_found_user = {
            need_snapshot_id_to_all_user[uid].screen_name.lower(): need_snapshot_id_to_all_user[uid] for uid in
            need_snapshot_id_to_all_user if utils.common.NOT_FOUND_TWITTER_USER_STR in uid}
        self.archive_old_users(need_snapshot_names_to_not_found_user, has_ids=False)

        # store new tweets for users with found id and CS_oldest_tweets_archived = PROCESSED
        # (if a user doesn't have a found id, then it is either deleted (NOT_FOUND) or suspended (SUSPENDED).
        # in both cases, we will not find tweets)
        # Max: I believe this stop is not necessary any more because it will be done by query_and_archive_tweets(frontfill)
        # need_new_tweets_users = [u for u in need_snapshot_users if
        #                          u.CS_oldest_tweets_archived == CS_JobState.PROCESSED.value and utils.common.NOT_FOUND_TWITTER_USER_STR not in u.id]
        # self.log.info("Need to get new tweets for {0} users".format(len(need_new_tweets_users)))
        # self.with_user_records_archive_tweets(need_new_tweets_users, is_test)  # TwitterUsers

    def archive_old_users(self, key_to_users, has_ids=True):
        """
            key_to_users = {user id (if has_ids is True) OR username (if has_ids is False): TwitterUser}
            we send {id: TwitterUser} if the user has an actual twitter id (the user is FOUND or PROTECTED)

            doesn't return anything
        """

        if len(key_to_users) <= 0:
            return None
        is_test = type(key_to_users) is not dict
        user_keys = list(key_to_users.keys())

        batch_size = 100  # limit should be 100
        # query batch_size at a time
        prev_limit = 0
        left_users = set(user_keys)

        for i in range(1, int(len(user_keys) / batch_size) + 2):
            limit = min(i * batch_size, len(user_keys))
            if limit > prev_limit:
                # query twitter API for user info
                users_info = []
                this_users = user_keys[prev_limit:limit]
                if len(this_users) > batch_size:
                    self.log.error(
                        "Caught error where this_users is too long??? : len(this_users) = {0}".format(len(this_users)))
                try:
                    if has_ids:
                        users_info = self.t.query(self.t.api.UsersLookup, user_id=this_users)
                    else:
                        users_info = self.t.query(self.t.api.UsersLookup, screen_name=this_users)
                except twitter.error.TwitterError as e:
                    self.t.try_counter = 0  ## this line prevents the retry code from looping
                    # this message means no users_info found: "[{'code': 17, 'message': 'No user matches for specified terms.'}]"
                    if e.message[0]['code'] != 17:
                        self.log.error(
                            "Unexpected error while querying for {0} Twitter users using api.UsersLookup: {1}; users: {2}".format(
                                limit - prev_limit, str(e), this_users))
                else:
                    self.log.info(
                        "Queried for {0} Twitter users out of a total of {1} users, got {2} out of {3} users".format(
                            limit, len(user_keys), len(users_info), limit - prev_limit))
                prev_limit = limit

                # for found users, commit to db

                users_json = [json.loads(json.dumps(user_info._json).encode("utf-8", "replace")) if type(
                    user_info) is twitter.models.User else user_info for user_info in
                              users_info]  # to accomodate test fixture data
                for user_json in users_json:

                    uid = str(user_json["id"])
                    screen_name = user_json["screen_name"].lower()

                    user_state = TwitterUserState.FOUND if not user_json["protected"] else TwitterUserState.PROTECTED
                    created_at = datetime.datetime.strptime(user_json["created_at"], utils.common.TWITTER_STRPTIME)
                    now = datetime.datetime.utcnow()

                    # get TwitterUser record
                    user = None
                    if has_ids:
                        user = key_to_users[uid]
                    else:
                        if screen_name in key_to_users:
                            # then screen_name hasn't changed. update the existing user record.
                            user = key_to_users[screen_name]
                        else:
                            # we don't expect to have called UsersLookup with some screen_names and get back any
                            # different screen_names if this did happen... we wouldn't have called UsersLookup with
                            # screen_names unless we didn't have the ids (users not found) if a previously not found
                            # user changed their screen name, AND their account got unsuspended, such that we are
                            # able to get their account info now, we'd get an id we haven't seen before, and there is
                            # NO WAY for us to match up these records. so we would want to create a new record.
                            self.log.error(
                                "Unexpected screen_name change: {0} is not in key_to_users. key_to_users' keys (screen_names): {1}".format(
                                    screen_name, key_to_users.keys()))

                    try:
                        if user:
                            # update TwitterUser record
                            user.id = uid
                            user.screen_name = screen_name
                            user.created_at = created_at
                            # user.record_updated_at = now    # THIS SHOULDN'T BE UPDATED. old TwitterUser records probably have wrong record_updated_at
                            user.lang = user_json["status"]["lang"] if 'status' in user_json.keys() and 'lang' in user_json['status'].keys() else None
                            user.user_state = user_state.value

                            # create TwitterUserSnapshot record
                            user_snapshot_record = TwitterUserSnapshot(
                                twitter_user_id=uid,
                                twitter_not_found_id=user.not_found_id,
                                # get from TwitterUser object. don't expect to not find TwitterUser object
                                record_created_at=now,
                                user_state=user_state.value,
                                user_json=json.dumps(user_json))  # already encoded
                            self.db_session.add(user_snapshot_record)

                    except:
                        self.log.error(
                            "Error while updating TwitterUser, creating TwitterUserSnapshot object for user {0}".format(
                                user_json["id"]), exc_info=True)
                    else:
                        if has_ids:
                            left_users.discard(uid)  # discard doesn't throw an error
                        else:
                            left_users.discard(screen_name)

                if len(users_info) > 0:
                    try:
                        self.db_session.commit()
                    except:
                        self.log.error(
                            "Error while saving DB Session for TwitterUser, TwitterUserSnapshot object for {0} users".format(
                                len(users_info)), exc_info=True)
                    else:
                        self.log.info("Saved {0} found twitter users' info.".format(len(users_info)))

        # at end, for left_users (users not found), commit to db
        for key in list(left_users):

            user = key_to_users[key]

            # disambiguate between NOT_FOUND, SUSPENDED
            user_state = self.is_user_suspended_or_deleted(user.screen_name)

            try:
                now = datetime.datetime.utcnow()
                # update TwitterUser record
                user.not_found_id = user.not_found_id if user.not_found_id else utils.common.generate_not_found_twitter_user_id(
                    user.screen_name)
                # user.record_updated_at = now        # TODO: fix this. models doesn't have this field right now
                user.user_state = user_state.value

                # create TwitterUserSnapshot record
                user_snapshot_record = TwitterUserSnapshot(
                    twitter_user_id=user.id,
                    twitter_not_found_id=user.not_found_id,
                    record_created_at=now,
                    user_state=user_state.value)
                self.db_session.add(user_snapshot_record)

            except:
                self.log.error(
                    "Error while updating TwitterUser, creating TwitterUserSnapshot object for user {0}".format(
                        user_json["id"]), exc_info=True)
        if len(left_users) > 0:
            try:
                self.db_session.commit()
            except:
                self.log.error("Error while saving DB Session for {0} not_found twitter users' info.".format(
                    len(left_users)), exc_info=True)
            else:
                self.log.info("Saved {0} not_found twitter users' info.".format(len(left_users)))

    #########################################################
    ################### ARCHIVE TWEET CODE
    #########################################################

    def query_and_archive_tweets(self, backfill=False, fill_start_time=None, collection_seconds=None,
                                 user_rand_frac=None,
                                 batch_size=10, order="ASC", test_exception=False, is_test=False):
        # make the order condition
        order_strat_map = {'ASC': TwitterUser.record_created_at.asc(),
                           'DESC': TwitterUser.record_created_at.desc()}
        order_strat = order_strat_map[order]

        # make the collection condition
        # only impose collection seconds if frontfill
        if backfill or collection_seconds is None:
            # otherwise we don't want to issue a condition so make this always True
            collection_condition = True
            self.log.info('Collection condition is: {0}'.format(collection_condition))
        elif collection_seconds:
            # their creation start
            self.log.info('Collection seconds are {0}'.format(collection_seconds))
            creation_deadline = fill_start_time - datetime.timedelta(seconds=collection_seconds)
            self.log.info('Collection deadline is: {0}'.format(creation_deadline))
            collection_condition = TwitterUser.record_created_at > creation_deadline
            self.log.info('Collection condition is: {0}'.format(collection_condition))
            # collection_eligible = self.db_session.query(TwitterUser).filter(collection_condition).count()
            # self.log.info('Collection eligible twitters users number: {0}'.format(collection_eligible))
            # # TODO: remove this colection condition overwrite.
            # self.log.info('Reminder, hacking collection condition to ensure it runs during Max holiday.'.format(collection_eligible))
            # collection_condition = True

        # make the backfill condition
        neq_or_eq = neq if backfill else eq

        target_JobState = CS_JobState.NOT_PROCESSED if backfill else CS_JobState.PROCESSED

        user_rand_frac = user_rand_frac if user_rand_frac else 1.0
        self.log.info('user_rand_frac is: {0}'.format(user_rand_frac))
        user_rand_condition = TwitterUser.user_rand <= user_rand_frac

        all_filled = False  # this flag gets set to True when we find no more users to fill

        # in a loop, until all_filled, get a batch and process the last_attempt_process state along with
        # fill_start_time, lets us know if that user has been attempted *this round*. The CS_JobState state let's us
        # know if every item was either processed successfully or failed
        batch_attempt_counter = 0
        while not all_filled:
            fill_query = self.db_session.query(TwitterUser). \
                filter(
                and_(
                    eq(TwitterUser.CS_oldest_tweets_archived, target_JobState.value),  # back or front
                    TwitterUser.lang.in_(EXPERIMENT_LANGUAGES),  # correct language
                    or_(eq(TwitterUser.last_attempted_process, None),
                        TwitterUser.last_attempted_process < fill_start_time).self_group(),
                    # or never been attempted yet
                    collection_condition,
                    user_rand_condition),
                ne(TwitterUser.user_state, utils.common.TwitterUserState.NOT_FOUND)
            ). \
                order_by(order_strat). \
                with_for_update(skip_locked=True). \
                limit(batch_size)

            # self.log.info('Fill query is: {}'.format(str(fill_query.statement.compile())))
            unarchived_users = fill_query.all()

            # mark in the database that we're claiming these items
            last_attempted_process = datetime.datetime.utcnow()
            for unarchived_user in unarchived_users:
                unarchived_user.last_attempted_process = last_attempted_process
                self.db_session.add_retryable(unarchived_user)
            self.db_session.commit()
            self.log.info(
                "About to query and archive tweets {0} users; backfill={1}".format(len(unarchived_users), backfill))
            # look for our loop ending condition which is that there are no more unarchived_users
            all_filled = True if len(unarchived_users) == 0 else False

            # set the current items to in-progress
            utils.common.update_CS_JobState(unarchived_users, "CS_oldest_tweets_archived", CS_JobState.IN_PROGRESS,
                                            self.db_session,
                                            self.log)
            # try to archive the users tweets
            try:
                self.with_user_records_archive_tweets(unarchived_users, backfill=backfill, is_test=is_test,
                                                      test_exception=test_exception,
                                                      fill_start_time=fill_start_time)  # backfill hacky
            # TODO if the user has become invalid then mark this
            except sqlalchemy.orm.exc.DetachedInstanceError:
                self.log.error("Encountered deatched instance error.")
            # finally reset in_progress and log
            finally:
                self.log.info('RESET CS_oldest_tweets_archived attempting.')
                utils.common.reset_CS_JobState_In_Progress(unarchived_users, "CS_oldest_tweets_archived",
                                                           self.db_session,
                                                           self.log)  # if still marked IN_PROGRESS (e.g. because of
                # unchecked exception), reset it to NOT_PROCESSED
                self.db_session.close()

            self.log.info(
                "PID {2} queried and archived tweets for {0} users; backfill={1}".format(len(unarchived_users),
                                                                                         backfill,
                                                                                         str(os.getpid())))
            self.log.info('PID {0} completed batch_attempt_counter={1} finding len(unarchived_users)={2}' \
                          .format(str(os.getpid()), batch_attempt_counter, len(unarchived_users)))

            batch_attempt_counter += 1

    def with_user_records_archive_tweets(self, user_records, backfill=False, is_test=False, test_exception=False,
                                         fill_start_time=None):
        """
            user_records: list of TwitterUser records

            returns user_to_state
        """

        if len(user_records) == 0:
            return

        if test_exception:
            counter = 0

        for user in user_records:
            # users's  user_state can change by executing the next line.
            job_state = self.archive_user_tweets(user, backfill=backfill, is_test=is_test)
            user.CS_oldest_tweets_archived = job_state.value
            self.db_session.add(user)
            self.db_session.commit()

            fill_record = TwitterFill(user_id=user.id,
                                      fill_start_time=fill_start_time,
                                      fill_type='backfill' if backfill else 'frontfill',
                                      job_state=job_state.value,
                                      user_state=user.user_state)
            self.db_session.add(fill_record)
            self.db_session.commit()

            if test_exception:
                counter += 1
                if counter >= len(user_records) / 2:
                    raise Exception("Throwing an exception for test purposes")

    def get_statuses_user_state(self, user_id, count=200, max_id=None, user_state=TwitterUserState.NOT_FOUND,
                                job_state=CS_JobState.FAILED):
        """
            returns (statuses, user_state, job_state)

            possible user_state: SUSPENDED, NOT_FOUND
        """
        (statuses, user_state, job_state) = ([], user_state, job_state)
        try:
            statuses = self.t.query(self.t.api.GetUserTimeline, user_id=user_id, count=count, max_id=max_id)
        except twitter.error.TwitterError as e:
            self.t.try_counter = 0  ## this line prevents the retry code from looping
            self.log.info(e)
            # TODO: un-jankify this error handling/parsing code. might not get much better though
            if e.message == "Not authorized.":
                # Account is either protected or suspended
                self.log.info(user_state is TwitterUserState.PROTECTED)
                if user_state is not TwitterUserState.PROTECTED:
                    user_state = TwitterUserState.SUSPENDED
            elif e.message[0]['code'] == 34:  # message = "Sorry, that page does not exist."
                user_state = TwitterUserState.NOT_FOUND
            else:
                self.log.error(
                    "Unexpected twitter.error.TwitterError exception while calling api.GetUserTimeline on user {0}: {1}".format(
                        user_id, e))
                job_state = CS_JobState.NEEDS_RETRY
        else:
            user_state = TwitterUserState.FOUND
        job_state = CS_JobState.PROCESSED
        return (statuses, user_state, job_state)

    def archive_user_tweets(self, user, backfill=False, is_test=False):
        """
            given TwitterUser user, archive user tweets.
            also updates TwitterUser record if unexpected user state, by calling self.archive_old_users
        """

        user_id = user.id

        if utils.common.NOT_FOUND_TWITTER_USER_STR in user_id or user.user_state is TwitterUserState.PROTECTED:
            # no tweets to be found with a NOT_FOUND id, or a protected user
            job_state = CS_JobState.PROCESSED
            return job_state

        job_state = CS_JobState.FAILED

        if backfill:
            # need to get all statuses
            query_seen_statuses = self.db_session.query(
                TwitterStatus.id).filter(
                TwitterStatus.user_id == user_id).all()
            seen_statuses = set([s[0] for s in query_seen_statuses if
                                 s is not None])  # set of ids already in db; s = (872295416376823808,)
        else:
            # believe that we have all statuses older than func.max(TwitterStatus.id),
            # so we only need to get func.max(TwitterStatus.id)
            query_seen_statuses = self.db_session.query(
                func.max(TwitterStatus.id)).filter(
                TwitterStatus.user_id == user_id).first()
            seen_statuses = set([query_seen_statuses])

        new_seen_statuses = set([])  # set of ids added this time

        oldest_id_queried = None  # if query_oldest_id is None else query_oldest_id[0]
        count = 200
        while True:

            # get statuses and job_state from twitter API. don't use user_state
            (statuses, user_state, sub_job_state) = self.get_statuses_user_state(user_id, count, oldest_id_queried,
                                                                                 user_state=TwitterUserState(
                                                                                     user.user_state),
                                                                                 job_state=CS_JobState.FAILED)

            if sub_job_state is not CS_JobState.PROCESSED:
                self.log.error(
                    "Unexpected error while calling api.GetUserTimeline on user_id {0}: sub_job_state is {1}".format(
                        user_id, sub_job_state))
                return sub_job_state
            if statuses is None:
                self.log.error(
                    "Unexpected error while calling api.GetUserTimeline on user_id {0}: nothing returned".format(
                        user_id))
                return job_state

            self.log.info(
                "PID {3} queried total of {0} tweets for account {1}. Backfill={2}".format(len(statuses), user_id,
                                                                                           str(backfill),
                                                                                           str(os.getpid())))

            if user_state is not TwitterUserState.FOUND:
                if not is_test:
                    # thought we probably had a found user, turns out we don't.
                    # to be safe, we should try to update our user records, create new snapshot
                    self.archive_old_users(key_to_users={user_id: user}, has_ids=True)
                else:
                    # if test, just update the user object here (since i don't want to mock out archive_old_users stuff...)
                    self.log.debug('In the test block with user: {0}'.format(user))
                    user.user_state = user_state.value
                    user.CS_oldest_tweets_archived = CS_JobState.PROCESSED.value
                    self.db_session.add(user)
                    self.db_session.commit()
                break

            if len(statuses) == 0:
                break

            # store TwitterStatus es
            statuses_jsons = [json.loads(json.dumps(status._json).encode("utf-8", "replace")) if type(
                status) is twitter.models.Status else status for status in statuses]  # to accomodate test fixture data]
            sorted_statuses_jsons = sorted(statuses_jsons, key=lambda s: datetime.datetime.strptime(s["created_at"],
                                                                                                    utils.common.TWITTER_STRPTIME))
            prev_new_seen_statuses_length = len(new_seen_statuses)
            this_oldest_id = min([status_json["id"] for status_json in sorted_statuses_jsons])

            status_records = []

            for i, status_json in enumerate(sorted_statuses_jsons):  # go through statuses from oldest to newest
                status_id = status_json["id"]
                created_at = datetime.datetime.strptime(status_json["created_at"], utils.common.TWITTER_STRPTIME)
                # if status hasn't been stored before, store
                if status_id not in seen_statuses and status_id not in new_seen_statuses:
                    try:
                        status_record = dict(
                            id=status_id,
                            user_id=str(status_json["user"]["id"]),
                            record_created_at=datetime.datetime.utcnow(),
                            created_at=created_at,  # "Sun Apr 16 17:11:30 +0000 2017"
                            status_data=json.dumps(status_json)
                        )
                        # status_record = TwitterStatus(
                        #     id = status_id,
                        #     user_id = str(status_json["user"]["id"]),
                        #     record_created_at = datetime.datetime.utcnow(),
                        #     created_at = created_at, #"Sun Apr 16 17:11:30 +0000 2017"
                        #     status_data = json.dumps(status_json))
                        # self.db_session.add(status_record)
                        status_records.append(status_record)
                        new_seen_statuses.add(status_id)
                    except:
                        self.log.error("Error while creating TwitterStatus object for user {0}, status id {1}".format(
                            status_json["user"]["id"]["screen_name"], status_id), exc_info=True)
                        return job_state
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", r"\(1062, \"Duplicate entry")
                    self.db_session.execute(TwitterStatus.__table__.insert().prefix_with("IGNORE"), status_records)
                    self.db_session.commit()
            except:
                self.log.error("Error while saving DB Session for {0} statuses for user {1}.".format(
                    len(new_seen_statuses) - prev_new_seen_statuses_length, user_id), exc_info=True)
                return job_state
            else:
                self.log.info("PID {2} Saved {0} statuses for user {1}.".format(
                    len(new_seen_statuses) - prev_new_seen_statuses_length, user_id, str(os.getpid())))

            if not backfill and this_oldest_id in seen_statuses:
                # if not backfill, we don't try to go back in time beyond the newest status we've already stored
                break
            elif oldest_id_queried is None or this_oldest_id < oldest_id_queried:
                # else, keep looking back until we don't get anymore new statuses from our query
                oldest_id_queried = this_oldest_id
            else:
                break

        job_state = CS_JobState.PROCESSED
        return job_state
