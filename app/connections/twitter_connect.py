import simplejson as json
import os, sys, glob, random, datetime, time, inspect, csv
from datetime import timedelta
from collections import defaultdict
import twitter
import app.cs_logger
# from retrying import retry
from time import sleep
from random import random

from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import func as sqlfunc

from app.models import TwitterToken, TwitterRateState

# this dict is accurate as of python-twitter 3.4.2
FUNC_ENDPOINTS = {'GetUserTimeline': '/statuses/user_timeline',
                  'UsersLookup': '/users/lookup',
                  'GetUser': '/users/show/:id',
                  'VerifyCredentials': '/account/verify_credentials',
                  'GetFriends': '/friends/list', }

ENV = os.environ['CS_ENV']


## HOW MANY TIMES TO RETRY?
## WE SHOULD RETRY FOR AS MANY TIMES AS THERE ARE KEYS
## NOTE: this was the only thing relying on the token_path being outside of the
## twitter_connect class, so I'm going to remove the dependency for now.
# RETRY_LIMIT = 100 #len(glob.glob(os.path.join(token_path, "*.json")))

# def rate_limit_retry(func):

#    def retry_if_api_limit_error(exception):
# print("rate_limit_retry: {0}".format(str(exception)))
# print(exception)
#        if(len(exception.args)>0 and len(exception.args[0])>0 and "code" in exception.args[0][0] and exception.args[0][0]['code'] == 88):
#            return True
# print("rate_limit_retry: Raising Exception")
#        raise exception

# this code wraps the function in a retry block
#    @retry(retry_on_exception=retry_if_api_limit_error, stop_max_attempt_number=RETRY_LIMIT)
#    def func_wrapper(self,*args, **kwargs):
# print("Before (Class {0}, Method {1})".format(self.__class__.__name__,  sys._getframe().f_code.co_name))
#        self.try_counter += 1
#        result = None
# try a new key only if it's the second attempt or later
# if(self.try_counter >= 2):
#     self.log.info("Twitter: rate limit calling TwitterConnect.api.{0} on ID {1}.".format(set(args).pop().__name__, self.curr_token.user_id))
#     ## reset time to be the appropriate reset time
#     ## by setting it to the earliest possible reset time
#     ## TODO: Make this more efficient by observing the specific
#     max_rate_limit = None
#     max_rate_limit_keys = []
#     for method, ratelist in self.api.rate_limit.resources.items():
#         for rl in list(ratelist.items()):
#             url = rl[0]
#             ratelimit = rl[1]
#             ##  TODO: THIS SHOULD BE THE LATEST RATE LIMIT FOR THIS KEY
#             if('reset' in ratelimit.keys() and (max_rate_limit is None or ratelimit['reset'] > max_rate_limit)):
#                 max_rate_limit_keys = [method, url]
#                 max_rate_limit = ratelimit['reset']
#     self.token['next_available'] = datetime.datetime.fromtimestamp(max_rate_limit)
#     self.token['available'] = False
#     self.log.info("Twitter: Token for ID {0} next available at {1}. Selecting a new token...".format(self.token['user_id'], self.token['next_available']))
#
#     # token = self.select_available_token()
#     previous_token_user = self.token['user_id']
#     if(self.apply_token(token)):
#         self.log.info("Twitter API connection verified under ID {0}. Previously {1}.".format(self.token['user_id'], previous_token_user))

#        result = func(self,*args, **kwargs)
## if the above line fails, the counter will iterate
## without being reset, since the line below would never run
## if the above line succeeds, reset the counter and continue
#        self.try_counter = 0
#        return result

#    return func_wrapper


class TwitterConnect():
    def __init__(self, log=None, db_session=None):
        BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "../..")
        self.try_counter = 0

        self.db_session = db_session
        self.api = None
        self.endpoint_tokens = {}
        self.curr_endpoint = None
        ## LOAD LOGGER
        if (log):
            self.log = log
        else:
            self.log = app.cs_logger.get_logger(ENV, BASE_DIR)

        ## LOAD INFORMATION ABOUT KEYS (relative or absolute path)
        config_path = os.path.join(BASE_DIR, "config", "twitter_configuration_" + ENV + ".json")
        with open(config_path, "r") as config:
            self.config = json.loads(config.read())

        if (self.config['key_path'][0] == "/"):
            self.token_path = self.config['key_path']
            self.log.info(
                "TwitterConnect is loading from an absolute configuration path specified in {0}".format(config_path))
        else:
            self.token_path = os.path.join(BASE_DIR, self.config['key_path'])
            self.log.info(
                "TwitterConnect is loading from a relative configuration path specified in {0}. Loading tokens from {1}".format(
                    config_path, self.token_path))

        self.update_db_tokens_from_files()

        ## LOAD BASE CONFIGURATION INFORMATION
        twitter_config_path = os.path.join(BASE_DIR, "config", "twitter_auth_" + ENV + ".json")
        with open(twitter_config_path, 'r') as t_config:
            twitter_config = json.loads(t_config.read())
        self.consumer_key = twitter_config["consumer_key"]
        self.consumer_secret = twitter_config["consumer_secret"]

        # do gymnastics to get `.api` attribute active.
        self.select_available_token('/account/verify_credentials')
        self.checkin_endpoint('/account/verify_credentials')
        # I think we won't intialize any token until the query is called.
        # self.curr_endpoint = self.select_available_token()
        #
        # self.log.info(f"Twitter connection initialized with and current token is: {self.curr_endpoint.user_id}")
        # if(self.apply_token(token)):
        #     self.log.info("Twitter API connection verified under ID {0}".format(self.token['user_id']))

    def update_db_tokens_from_files(self):
        '''
        This function figures out which tokens are in the token_path but not
        in the database, and then loads in the ones the missing ones.
        In the future it will also set the deleted files to inactive in the DB.
        '''
        # get token names from key_path
        token_path_names = os.listdir(self.token_path)
        dir_tokens = set([fname.split('.json')[0] for fname in token_path_names \
                          if fname.endswith('.json')])
        self.log.info('Found {0} tokens in {1}'.format(len(dir_tokens), self.token_path))
        # get the tokens currently in the database #just the usernames
        db_tokens_res = self.db_session.query(TwitterToken).options(load_only('username')).all()
        db_tokens = set([r.username for r in db_tokens_res])
        self.log.info('Found {0} tokens in twitter_tokens table'.format(len(db_tokens)))

        # do some set subtraction in both directions
        in_dir_not_db = dir_tokens - db_tokens
        in_db_not_dir = db_tokens - dir_tokens
        self.log.info('Found {0} tokens in directory not db'.format(len(in_dir_not_db)))
        self.log.info('Found {0} tokens in db not directory'.format(len(in_db_not_dir)))

        # add all tokens not already in db
        tokens_to_add = []
        ratestates_to_add = []
        creation_time_epsilon = datetime.datetime.now() - timedelta(seconds=1)  # a little in the past
        self.log.info('Creation time is {0}'.format(creation_time_epsilon))
        for token_username in in_dir_not_db:
            with open(os.path.join(self.token_path, '{0}.json'.format(token_username)), 'r') as f:
                token_data = json.load(f)

                token_obj = TwitterToken()
                token_obj.username = token_data['username']
                token_obj.user_id = token_data['user_id']
                token_obj.oauth_token = token_data['oauth_token']
                token_obj.oauth_token_secret = token_data['oauth_token_secret']
                tokens_to_add.append(token_obj)

                for endpoint in FUNC_ENDPOINTS.values():
                    ratestate = TwitterRateState()
                    ratestate.user_id = token_data['user_id']
                    ratestate.endpoint = endpoint  # special value
                    ratestate.checkin_due = creation_time_epsilon  # it will immediatley be available
                    ratestate.reset_time = creation_time_epsilon  # likewise immediately out of date
                    ratestate.limit = -1  # special creation value
                    ratestate.remaining = -1  # special creation value
                    ratestate.resources = '{}'
                    ratestates_to_add.append(ratestate)

        # Twitter Tokens table
        self.db_session.add_all(tokens_to_add)
        self.db_session.commit()
        self.log.info('Added {0} tokens to twitter_tokens table'.format(len(tokens_to_add)))

        # Twitter RateState tables
        self.db_session.add_all(ratestates_to_add)
        self.db_session.commit()
        self.log.info('Added {0} tokens to twitter_ratestate table'.format(len(ratestates_to_add)))

        # at least log what's in but not in dir
        for token_username in in_db_not_dir:
            self.log.info("I think {0} has revoked permission and\
            we should set their token to inactive.".format(token_username))

    ## This method takes a token and tries to adjust the API to query using the token
    def apply_token(self, endpoint):
        conn_args = {'consumer_key': self.consumer_key,
                     'consumer_secret': self.consumer_secret,
                     'access_token_key': self.endpoint_tokens[endpoint].oauth_token,
                     'access_token_secret': self.endpoint_tokens[endpoint].oauth_token_secret}
        if (self.api is None):
            self.api = twitter.Api(**conn_args)
        else:
            self.api.SetCredentials(**conn_args)
        try:
            verification = self.api.VerifyCredentials()
            self.api.InitializeRateLimit()  # dangerous for us.
        except twitter.error.TwitterError as e:
            self.log.error(
                "Twitter: Failed to connect to API with endpoint {0}. Remove from token set. Error: {1}.".format(
                    endpoint, str(e)))
            if e[0]['code'] == 89:  # or 'message': 'Invalid or expired token.':
                raise NotImplementedError('mark as invalid')
            return False
        self.curr_endpoint = endpoint
        return True

    ## This method will select from available tokens
    ## or if no tokens are available, wait until the next token
    ## becomes available, based on information from the Twitter API
    ## then return that token
    def select_available_token(self, endpoint, strategy='sequential'):
        wait_before_return = 0
        succeeded = False
        strategy_order = {'random': sqlfunc.rand(),
                          'sequential': TwitterRateState.user_id,
                          # another strategy might be fetch the most remaining in the future
                          }
        order_by = strategy_order[strategy]
        self.log.info('order strategy is {0}: giving: {1}'.format(strategy, order_by))
        while not succeeded:
            query_time = datetime.datetime.now()
            try:
                # 2. find first token-endpoint where
                # endpoint matches
                # token-endpoint not checked out
                # token-endpoint after reset_time
                endpoint_select = self.db_session.query(TwitterRateState) \
                    .filter(TwitterRateState.endpoint == endpoint) \
                    .filter(TwitterRateState.checkin_due < query_time) \
                    .filter(TwitterRateState.reset_time < query_time) \
                    .order_by(order_by) \
                    .with_for_update().first()
                self.log.info('''Trying to get token matching \
                                  endpoint: {0} \
                                  query_time: {1}'''.format(endpoint, query_time))
                self.log.info('Number Token-endpoint query results: {0}'.format(1 if endpoint_select else 0))

                # 3 check if the endpoint_select is empty
                if not endpoint_select:
                    self.db_session.rollback()
                    # now there are two cases. Either
                    # a) all checked-out or b) all reset_time
                    # start with most common a) we can pre-book a not-checked out token
                    prebook = self.db_session.query(TwitterRateState) \
                        .filter(TwitterRateState.endpoint == endpoint) \
                        .filter(TwitterRateState.checkin_due < query_time) \
                        .order_by(TwitterRateState.reset_time).first()
                    if prebook:
                        wait_before_return = (prebook.reset_time - query_time).total_seconds()
                        self.log.info(
                            'This is a prebook situation, not available until seconds: {0}'.format(wait_before_return))
                        self.log.info('Prebook user_id is {0}'.format(prebook.user_id))
                    if not prebook:
                        # else we need to b) keep on waiting until we can checksomething out
                        next_checkout = self.db_session.query(TwitterRateState) \
                            .filter(TwitterRateState.endpoint == endpoint) \
                            .order_by(TwitterRateState.checkin_due).first()
                        # add a bit of noise for loop until
                        time_until_next_try = next_checkout.checkin_due - query_time + timedelta(seconds=random())
                        self.log.info(
                            'PID {1}: Oh dear all the endpoints are checked out for at least seconds: {0}'.format(
                                time_until_next_try, str(os.getpid())))
                        sleep(time_until_next_try.total_seconds())
                        continue
                # 4. update checkout_due in database for select token-endpoint
                assert endpoint_select or prebook
                # either a token, or the prebook
                token_endpoint = endpoint_select if endpoint_select else prebook
                token_endpoint.checkin_due = query_time + timedelta(minutes=60 * 24)  # 1 day loan
                self.db_session.add(token_endpoint)
                self.db_session.commit()
                self.log.debug("I think I commited the checkin_due update")
                token = self.db_session.query(TwitterToken).filter(TwitterToken.user_id == token_endpoint.user_id).one()
                self.db_session.commit()
                # class dictionary update
                self.endpoint_tokens[endpoint] = token
                self.curr_endpoint = endpoint
                self.log.debug('wiating for {0}'.format(wait_before_return))
                sleep(wait_before_return)
                self.apply_token(endpoint)
                return True
            except:
                self.log.exception('exception in getting from DB for tokens')
                self.db_session.rollback()
                raise

    def get_reset_time_of_endpoint(self, endpoint):
        '''Utility to walk through the rate_limit dict
           until we find the endpoint and return its reset time'''
        # NOTE Future-me, not 100% sure that this dict is always in sync with reality
        for groupname, groupdict in self.api.rate_limit.resources.items():
            if endpoint in groupdict.keys():
                return groupdict[endpoint]['reset']

    def get_ratestate_of_endpoint(self, endpoint):
        user_id = self.endpoint_tokens[endpoint].user_id
        # construct the ratestate object with for update
        # remember that the key of RateState is a combination of endpoint and user_id
        # so this should be unique
        ratestate = self.db_session.query(TwitterRateState).filter(TwitterRateState.user_id == user_id) \
            .filter(TwitterRateState.endpoint == endpoint).with_for_update().one()
        self.log.debug('ratestate object had user_id:{0}'.format(ratestate.user_id))
        return ratestate

    def checkin_endpoint(self, endpoint=None):
        if endpoint is None:
            assert self.curr_endpoint, 'There was no curr_endpoint to use as default'
            endpoint = self.curr_endpoint
        ratestate = self.get_ratestate_of_endpoint(endpoint)
        checkin_time = datetime.datetime.now()
        ratestate.checkin_due = checkin_time
        self.db_session.add(ratestate)
        self.db_session.commit()

        # delete form local records
        del self.endpoint_tokens[endpoint]
        self.curr_endpoint = None

    def mark_reset_time(self, endpoint):
        reset_time = self.get_reset_time_of_endpoint(endpoint)
        ratestate = self.get_ratestate_of_endpoint(endpoint)
        self.log.debug(
            'Marking exhausted user_id:{0}, endpoint:{1}, reset_time{2}.'.format(ratestate.user_id, endpoint,
                                                                                 reset_time))
        # put in the reset time
        ratestate.reset_time = datetime.datetime.fromtimestamp(reset_time)
        self.db_session.add(ratestate)
        self.db_session.commit()

    def mark_reset_time_and_checkin(self, endpoint):
        self.mark_reset_time(endpoint)
        self.checkin_endpoint(endpoint)

    # @rate_limit_retry
    def query(self, method, *args, **kwargs):
        method_name = method.__name__
        # find the endpoint that will be used
        endpoint = FUNC_ENDPOINTS[method_name]
        # switch to that token or select_available_token
        if endpoint in self.endpoint_tokens.keys():
            if endpoint == self.curr_endpoint:
                pass  # no switching necessary
            else:
                # activate this credential if its not the active one
                self.apply_token(endpoint)
        # we need to get a token-endpoint from the database
        else:
            self.select_available_token(endpoint)
            # select available tokens
        try:
            # try to actually execute
            result = method(*args, **kwargs)
        # if we get a an error from twitter
        except twitter.TwitterError as twiterr:
            # if it's rate exceeded we know how to deal with that
            if type(twiterr.message).__name__ == "list" and twiterr.message[0]['message'] == 'Rate limit exceeded':
                self.log.info('Rate limit encountered on endpoint:{0}'.format(endpoint))
                self.mark_reset_time_and_checkin(endpoint)
                # recurse!
                self.log.info('Recursing for method:')
                return self.query(method, *args, **kwargs)
            else:
                self.log.info(
                    'Twitter Query encountered twitter error other than Rate Limit Exceeded: {0}'.format(twiterr))
                raise twiterr
        return result
