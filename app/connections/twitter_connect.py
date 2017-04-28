import requests
import simplejson as json
import os, inspect
import twitter

ENV =  os.environ['CS_ENV']

class TwitterConnect():
    def __init__(self, log):
        BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
        twitter_config_path = os.path.join(BASE_DIR, "config", "twitter_auth_" + ENV + ".json")
        
        with open(twitter_config_path, 'r') as config:
            TWITTERCONFIG = json.loads(config.read())

        self.api = twitter.Api(consumer_key=TWITTERCONFIG["consumer_key"],
                          consumer_secret=TWITTERCONFIG["consumer_secret"],
                          access_token_key=TWITTERCONFIG["access_token_key"],
                          access_token_secret=TWITTERCONFIG["access_token_secret"],
                          sleep_on_rate_limit=True)