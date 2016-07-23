import os, sys
BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../", "../")
sys.path.append(BASE_DIR)

import reddit.connection
from app.models import PrawKey
from utils.common import DbEngine
from praw.handlers import MultiprocessHandler
import praw
import simplejson as json

ENV = os.environ['CS_ENV']
db_session = DbEngine(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV)).new_session()

for pk in db_session.query(PrawKey).all():
  handler = MultiprocessHandler()
  r = praw.Reddit(user_agent="Test version of CivilServant by u/natematias", handler=handler)

  access_information = {}
  access_information['access_token'] = pk.access_token
  access_information['refresh_token'] = pk.refresh_token
  access_information['scope'] = json.loads(pk.scope)

  r.set_access_credentials(**access_information)

  print("USER {0}, ID {1}".format(r.user.json_dict['name'], r.user.json_dict['id']))
  pk.authorized_username = r.user.json_dict['name']
  pk.authorized_user_id = r.user.json_dict['id']

db_session.commit()
