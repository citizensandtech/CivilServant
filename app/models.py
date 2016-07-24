# example taken from http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
import os
import sys
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
import sqlalchemy
import datetime
import socket

Base = declarative_base()

class FrontPage(Base):
    __tablename__ = 'front_pages'
    id                  = Column(Integer, primary_key = True)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    page_type           = Column(Integer) # see utils/common.py
    page_data           = Column(MEDIUMTEXT)

class Subreddit(Base):
    __tablename__ = 'subreddits'
    id                  = Column(String(32), primary_key = True, unique=True, autoincrement=False) # subreddit id
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    name                = Column(String(32)) # display_name

class SubredditPage(Base):
    __tablename__ = 'subreddit_pages'
    id                  = Column(Integer, primary_key = True)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    subreddit_id        = Column(String(32))
    page_type           = Column(Integer) # see utils/common.py
    page_data           = Column(MEDIUMTEXT)

class Post(Base):
    __tablename__ = 'posts'
    id                  = Column(String(32), primary_key = True, unique=True, autoincrement=False)	# post id
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    subreddit_id        = Column(String(32))	# "subreddit_id"
    created             = Column(DateTime) # "created"
    #when this record was created:
    created_at          = Column(DateTime, default=datetime.datetime.utcnow) 
    post_data           = Column(MEDIUMTEXT)	# "json_dict"
    comment_data        = Column(LONGTEXT)
    comments_queried_at = Column(DateTime, default=None)  

class ModAction(Base):
    __tablename__ = "mod_actions"
    id                  = Column(String(256), primary_key = True, unique=True, autoincrement=False)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)  
    created_utc         = Column(DateTime)
    subreddit_id        = Column(String(32), index=True)
    mod                 = Column(String(64))
    target_author       = Column(String(64), index=True)
    action              = Column(String(256))
    target_fullname     = Column(String(256))
    action_data         = Column(MEDIUMTEXT) # json_dict

class Comment(Base):
    __tablename__ = "comments"
    id                  = Column(String(256), primary_key = True, unique=True, autoincrement=False)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)  
    created_utc         = Column(DateTime)
    subreddit_id        = Column(String(32), index=True)
    post_id             = Column(String(32), index=True)
    user_id             = Column(String(64), index=True)
    comment_data        = Column(MEDIUMTEXT)

class User(Base):
    __tablename__ = 'users'
    name                = Column(String(32), primary_key = True, unique=True, autoincrement=False)   # redditor's name
    id                  = Column(String(32)) # "redditor's id"
    created             = Column(DateTime) # "created"
    first_seen          = Column(DateTime)
    last_seen           = Column(DateTime)
    user_data           = Column(MEDIUMTEXT)

class PrawKey(Base):
    __tablename__ = 'praw_keys'
    # IDs will be a string based on the assumption
    # that each device will only have one process
    # at a time handling a particular controller
    # in the format:  
    #   HOST:ENV:CONTROLLER
    # For example:
    #   hannahmore:development:FrontPageController
    id                  = Column(String(256), primary_key = True)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    access_token        = Column(String(256))
    scope               = Column(String(256)) #json
    refresh_token       = Column(String(256))
    authorized_username = Column(String(256), index=True)
    authorized_user_id  = Column(String(256), index=True)

    @classmethod
    def get_praw_id(cls, env, controller):
        host = socket.gethostname()
        return "{0}:{1}:{2}".format(host,env,controller)

class Experiment(Base):
    __tablename__ = 'experiments'
    id                  = Column(Integer, primary_key = True)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    name                = Column(String(256), nullable=False, index=True)
    controller          = Column(String(256), nullable=False)
    start_time          = Column(DateTime)
    end_time            = Column(DateTime)
    settings_json       = Column(MEDIUMTEXT)

class ExperimentThing(Base):
    __tablename__ = 'experiment_things'
    id                  = Column(String(256), primary_key = True)
    created_at          = Column(DateTime, default = datetime.datetime.utcnow)
    object_type         = Column(Integer)
    experiment_id       = Column(Integer, index = True)
    object_created      = Column(DateTime)
    metadata_json       = Column(MEDIUMTEXT)

class ExperimentAction(Base):
    __tablename__ = 'experiment_actions'
    id                  = Column(Integer, primary_key = True)
    created_at          = Column(DateTime, default = datetime.datetime.utcnow)
    experiment_id       = Column(Integer, index = True)
    praw_key_id         = Column(String(256), index = True)
    action              = Column(String(64), index = True)
    action_subject_type = Column(String(64))
    action_subject_id   = Column(String(256), index=True)
    action_object_type  = Column(String(64))
    action_object_id    = Column(String(256), index=True)
    metadata_json       = Column(MEDIUMTEXT)

    



