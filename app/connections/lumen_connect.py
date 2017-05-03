import requests
import simplejson as json
import os, inspect
import utils.common
import time
#import pickle
#from app.models import Base
#from sqlalchemy import create_engine
#from sqlalchemy.orm import sessionmaker
#import sqlalchemy
#from utils.common import DbEngine

ENV =  os.environ['CS_ENV']

class LumenConnect():
    def __init__(self, log):
        BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "../..")
        lumen_config_path = os.path.join(BASE_DIR, "config") + "/lumen_auth_" + ENV + ".json"

        with open(lumen_config_path, 'r') as config:
          LUMENCONFIG = json.loads(config.read())

        self.headers = {
            "Content-type": "application/json",
            "Accept": "application/json",
            "X-Authentication-Token": LUMENCONFIG["X-Authentication-Token"]
        } 
        self.log = log

    def get(self, url, payload):
        retries = 3
        while retries > 0:
            r = requests.get(url, 
                params=payload,
                headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                retries -= 1
                self.log.error("Error querying usernames with notes. Status code {0}. Retrying ({1} retries left)".format(r.status_code, retries))
                time.sleep(30) # "If you do not have a researcher API token you will limited to 25 results per request and 3 requests per minute. "

    def get_search(self, payload):
        return self.get("https://Lumendatabase.org/notices/search", payload)

    def get_notices_to_twitter(self, topics, count, page, from_date, to_date):
        payload = {
            "topics": topics,
            "per_page": count,
            "page": page,
            "sort_by": "date_received desc",
            "recipient_name": "Twitter",
            "date_received_facet": {
                "from": utils.common.time_since_epoch_ms(from_date),
                "to": utils.common.time_since_epoch_ms(to_date)
            }
        }

        return self.get_search(payload)
