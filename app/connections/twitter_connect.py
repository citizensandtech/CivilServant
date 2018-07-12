import simplejson as json
import os, sys, glob, random, datetime, time, inspect, csv
from collections import defaultdict
import twitter
import app.cs_logger
from retrying import retry

from sqlalchemy.orm import load_only

from app.models import TwitterToken, TwitterRateState

ENV =  os.environ['CS_ENV']

## HOW MANY TIMES TO RETRY?
## WE SHOULD RETRY FOR AS MANY TIMES AS THERE ARE KEYS
## NOTE: this was the only thing relying on the token_path being outside of the
## twitter_connect class, so I'm going to remove the dependency for now.
RETRY_LIMIT = 100 #len(glob.glob(os.path.join(token_path, "*.json")))

def rate_limit_retry(func):

    def retry_if_api_limit_error(exception):
        #print("rate_limit_retry: {0}".format(str(exception)))
        #print(exception)
        if(len(exception.args)>0 and len(exception.args[0])>0 and "code" in exception.args[0][0] and exception.args[0][0]['code'] == 88):
            return True
        #print("rate_limit_retry: Raising Exception")
        raise exception

    # this code wraps the function in a retry block
    @retry(retry_on_exception=retry_if_api_limit_error, stop_max_attempt_number=RETRY_LIMIT)
    def func_wrapper(self,*args, **kwargs):
        #print("Before (Class {0}, Method {1})".format(self.__class__.__name__,  sys._getframe().f_code.co_name))
        self.try_counter += 1
        result = None
        #try a new key only if it's the second attempt or later
        if(self.try_counter >= 2):
            self.log.info("Twitter: rate limit calling TwitterConnect.api.{0} on ID {1}.".format(set(args).pop().__name__, self.token['user_id']))
            ## reset time to be the appropriate reset time
            ## by setting it to the earliest possible reset time
            ## TODO: Make this more efficient by observing the specific
            max_rate_limit = None
            max_rate_limit_keys = []
            for method, ratelist in self.api.rate_limit.resources.items():
                for rl in list(ratelist.items()):
                    url = rl[0]
                    ratelimit = rl[1]
                    ##  TODO: THIS SHOULD BE THE LATEST RATE LIMIT FOR THIS KEY
                    if('reset' in ratelimit.keys() and (max_rate_limit is None or ratelimit['reset'] > max_rate_limit)):
                        max_rate_limit_keys = [method, url]
                        max_rate_limit = ratelimit['reset']
            self.token['next_available'] = datetime.datetime.fromtimestamp(max_rate_limit)
            self.token['available'] = False
            self.log.info("Twitter: Token for ID {0} next available at {1}. Selecting a new token...".format(self.token['user_id'], self.token['next_available']))

            token = self.select_available_token()
            previous_token_user = self.token['user_id']
            if(self.apply_token(token)):
                self.log.info("Twitter API connection verified under ID {0}. Previously {1}.".format(self.token['user_id'], previous_token_user))

        result = func(self,*args, **kwargs)
        ## if the above line fails, the counter will iterate
        ## without being reset, since the line below would never run
        ## if the above line succeeds, reset the counter and continue
        self.try_counter = 0
        return result

    return func_wrapper


class TwitterConnect():
    def __init__(self, log=None, db_session=None):
        BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "../..")
        self.try_counter = 0

        self.db_session = db_session
        self.api = None
        self.token_endpoints = {}
        self.curr_token = {}
        ## LOAD LOGGER
        if(log):
            self.log = log
        else:
            self.log = app.cs_logger.get_logger(ENV, BASE_DIR)

        ## LOAD INFORMATION ABOUT KEYS (relative or absolute path)
        config_path = os.path.join(BASE_DIR, "config", "twitter_configuration_" + ENV + ".json")
        with open(config_path, "r") as config:
            self.config = json.loads(config.read())

        if(self.config['key_path'][0] == "/"):
            self.log.info("TwitterConnect is loading from an absolute configuration path specified in {0}".format(config_path))
            self.token_path = self.config['key_path']
        else:
            self.log.info("TwitterConnect is loading from a relative configuration path specified in {0}. Loading tokens from {1}".format(config_path, token_path))
            self.token_path = os.path.join(BASE_DIR, self.config['key_path'])

        self.update_db_tokens_from_files()

        ## LOAD BASE CONFIGURATION INFORMATION
        twitter_config_path = os.path.join(BASE_DIR, "config", "twitter_auth_" + ENV + ".json")
        with open(twitter_config_path, 'r') as t_config:
            twitter_config = json.loads(t_config.read())
        self.consumer_key = twitter_config["consumer_key"]
        self.consumer_secret = twitter_config["consumer_secret"]

        from IPython import embed; embed()

        self.curr_token = self.select_available_token()

        self.log.info(f"Twitter connection initialized with and current token is: {self.curr_token.user_id}")
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
        self.log.info(f'Found {len(dir_tokens)} tokens in f{self.token_path}')
        # get the tokens currently in the database #just the usernames
        db_tokens_res = self.db_session.query(TwitterToken).options(load_only('username')).all()
        db_tokens = set([r.username for r in db_tokens_res])
        self.log.info(f'Found {len(db_tokens)} tokens in twitter_tokens table')

        #do some set subtraction in both directions
        in_dir_not_db = dir_tokens - db_tokens
        in_db_not_dir = db_tokens - dir_tokens
        self.log.info(f'Found {len(in_dir_not_db)} tokens in directory not db')
        self.log.info(f'Found {len(in_db_not_dir)} tokens in db not directory')

        # add all tokens not already in db
        tokens_to_add = []
        ratestates_to_add = []
        for token_username in in_dir_not_db:
            with open(os.path.join(self.token_path, f'{token_username}.json'), 'r') as f:
                token_data = json.load(f)

                token_obj = TwitterToken()
                token_obj.username = token_data['username']
                token_obj.user_id = token_data['user_id']
                token_obj.oauth_token = token_data['oauth_token']
                token_obj.oauth_token_secret = token_data['oauth_token_secret']
                tokens_to_add.append(token_obj)

                for endpoint in ['/account/verify_credentials', '/users/lookup',
                                 '/users/show/:id', '/statuses/user_timeline']:
                    ratestate = TwitterRateState()
                    ratestate.user_id = token_data['user_id']
                    ratestate.endpoint = endpoint #special value
                    ratestate.checkin_due = datetime.datetime.now() #it will immediatley be available
                    ratestate.reset_time = datetime.datetime.now()  #likewise immediately out of date
                    ratestate.limit = -1 #special creation value
                    ratestate.remaining = -1 #special creation value
                    ratestate.resources = '{}'
                    ratestates_to_add.append(ratestate)

        # Twitter Tokens table
        self.db_session.add_all(tokens_to_add)
        self.db_session.commit()
        self.log.info(f'Added {len(tokens_to_add)} tokens to twitter_tokens table')

        # Twitter RateState tables
        self.db_session.add_all(ratestates_to_add)
        self.db_session.commit()
        self.log.info(f'Added {len(ratestates_to_add)} tokens to twitter_ratestate table')

        # at least log what's in but not in dir
        for token_username in in_db_not_dir:
            self.log.info(f"I think {token_username} has revoked permission and\
            we should set their token to inactive.")

    ## This method takes a token and tries to adjust the API to query using the token
    def apply_token(self, token):
        conn_args = {'consumer_key':self.consumer_key,
                     'consumer_secret':self.consumer_secret,
                     'access_token_key':token['oauth_token'],
                     'access_token_secret':token['oauth_token_secret']}
        if(self.api is None):
            self.api = twitter.Api(**conn_args)
        else:
            self.api.SetCredentials(**conn_args)
        try:
            verification = self.api.VerifyCredentials()
            self.api.InitializeRateLimit() #dangerous for us.
        except twitter.error.TwitterError as e:
            self.log.error("Twitter: Failed to connect to API with User ID {0}. Remove from token set. Error: {1}.".format(token['user_id'], str(e)))
            self.curr_token['valid'] = False
            self.token = None
            return False
        self.token = token
        return True

    ## This method will select from available tokens
    ## or if no tokens are available, wait until the next token
    ## becomes available, based on information from the Twitter API
    ## then return that token
    def select_available_token(self):
        available_tokens = [token for token in self.tokens.values() if (token['available'] and token['valid'])]
        ## we take the first one rather than a random sample
        ## to make testing more reliable
        available_token = None
        if(len(available_tokens)>0):
            available_token = available_tokens[0]

        if(available_token is None):
            available_tokens = sorted(list(self.tokens.values()), key=lambda x: x['next_available'])
            for token in available_tokens:
                seconds_until_available = (token['next_available'] - datetime.datetime.now()).total_seconds() + 1
                if(seconds_until_available <= 0):
                    token['available'] = True

            try:
                available_token = available_tokens[0]
            except:
                self.log.error("Twitter: failed to find any valid tokens. Ending process.")
                sys.exit("Twitter: failed to find any valid tokens. Ending process")

            seconds_until_available = (available_token['next_available'] - datetime.datetime.now()).total_seconds() + 1
            if(seconds_until_available>0):
                self.log.info("Twitter: Next available token ({0}): {1} seconds. Waiting...".format(available_token['user_id'], seconds_until_available))
                time.sleep(seconds_until_available)
        return available_token

    ## TO USE RATE LIMIT MULTIPLEXING, CALL THE BELOW METHOD AS FOLLOWS
    ## x = TwitterConnect()
    ## x.query(x.api.GetFriends, "user")
    @rate_limit_retry
    def query(self, method, *args, **kwargs):
        method_name = method.__name__
        #find the endpoint that will be used

        #select available tokens
        result = method(*args, **kwargs)

        return result
