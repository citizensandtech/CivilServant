import praw
import pickle
import os, inspect
from app.models import PrawKey
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base
import sqlalchemy
import utils.common

ENV =  os.environ['CS_ENV']

class Connect:

  # this initializer accepts a database session
  # and if it doesn't exist, initializes one
  # this may become a pattern that we should break out
  # into its own class eventually
  def __init__(self, db_session = None, base_dir="", env=None):
    self.base_dir = base_dir
    if(env):
      self.env = env
    else:
      self.env = ENV
    ## LOAD DATABASE OBJECT

    if db_session is None:
      ### LOAD SQLALCHEMY SESSION
      self.db_session = utils.common.DbEngine(os.path.join(self.base_dir, "config","{env}.json".format(env=self.env))).new_session()
    else:
      self.db_session = db_session
    
  def connect(self, controller="Main"):
    r = None #Praw Connection Object

    # Check the Database for a Stored Praw Key
    db_praw_id = PrawKey.get_praw_id(self.env, controller)
    pk = self.db_session.query(PrawKey).filter_by(id=db_praw_id).first()
    
    access_information = {}
    
    if(pk is None):
      with open(os.path.join(self.base_dir, "config","access_information_praw4_{environment}.pickle".format(environment=self.env)), "rb") as fp:
          access_information = pickle.load(fp)
    else:
      access_information['access_token'] = pk.access_token
      access_information['refresh_token'] = pk.refresh_token
      access_information['scope'] = json.loads(pk.scope)
    
    r = praw.Reddit(refresh_token=access_information['refresh_token'])

    # praw4 TODO: check that this code works
    # SAVE OR UPDATE THE ACCESS TOKEN IF NECESSARY
    if(pk is None):
      pk = PrawKey(id=db_praw_id,
                   access_token = r.config.access_token,
                   refresh_token = r.config.refresh_token,
                   scope = json.dumps(r.config.scope),
                   authorized_username = r.config.username,
                   authorized_user_id = r.user.json_dict['id']) # TODO: broken
      self.db_session.add(pk)

    elif(access_information['access_token'] != r.config.access_token or
       access_information['refresh_token'] != r.config.refresh_token):
       pk.access_token = r.config.access_token
       pk.refresh_token = r.config.refresh_token
       pk.scope = json.dumps(r.config.scope)
      
    self.db_session.commit()
    return r
