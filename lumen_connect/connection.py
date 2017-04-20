import requests
import simplejson as json
import os, inspect
#import pickle
#from app.models import Base
#from sqlalchemy import create_engine
#from sqlalchemy.orm import sessionmaker
#import sqlalchemy
#from utils.common import DbEngine

ENV =  os.environ['CS_ENV']

class LumenConnect():
  def __init__(self, log):
    BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
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
    r = requests.get(url, 
        params=payload,
        headers=self.headers)
    if r.status_code == 200:
        return json.loads(r.text)
    else:
        self.log.error("Error querying usernames with notes. Status code {0}".format(r.status_code))

  def get_search(self, payload):
    return self.get("https://Lumendatabase.org/notices/search", payload)