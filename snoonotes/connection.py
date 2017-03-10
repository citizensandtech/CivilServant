import requests
import pickle
import os, inspect
from app.models import Base, SNote
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from utils.common import DbEngine

#ENV =  os.environ['CS_ENV']

class SnooNotesConnect:
  def __init__(self):
    self.headers = {
      'User-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:51.0) Gecko/20100101 Firefox/51.0",
      'content-type': 'application/json'}
    self.baseurl = "https://snoonotes.com"

    BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
    config_path = os.path.join(BASE_DIR, "config") + "/snoonotes.json"
    with open(config_path, "r") as config:
        SNCONFIG = json.loads(config.read())

    self.cookies = {"ARRAffinity": SNCONFIG["ARRAffinity"],
               "snPrefs": SNCONFIG["snPrefs"],
               "bog": SNCONFIG["bog"]}

  def get_users(self):
    url = self.baseurl + '/api/Note/GetUsernamesWithNotes'
    res = requests.get(url, headers=self.headers, cookies=self.cookies)
    if res.status_code == 200:
      return json.loads(res.text)
    else:
      print("Error querying usernames with notes. Status code {0}".format(res.status_code))

  def post_get_notes(self, users):
    url = self.baseurl + "/api/Note/GetNotes"
    res = requests.post(url, cookies=self.cookies, headers=self.headers, json=users)
    if res.status_code == 200:
      return json.loads(res.text)
    else:
      print("Error querying usernames with notes. Status code {0}".format(res.status_code))

  def get_schemas(self):
    url = self.baseurl + "/restapi/Subreddit"
    res = requests.get(url, cookies=self.cookies, headers=self.headers)
    if res.status_code == 200:
      return json.loads(res.text)
    else:
      print("Error querying data. Status code {0}".format(res.status_code))