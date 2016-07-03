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
    id = Column(Integer, primary_key = True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    page_type = Column(Integer) # see utils/common.py
    page_data = Column(MEDIUMTEXT)

class Subreddit(Base):
    __tablename__ = 'subreddits'
    id = Column(String(32), primary_key = True, unique=True, autoincrement=False) # reddit's id
    name = Column(String(32)) # display_name

class SubredditPage(Base):
    __tablename__ = 'subreddit_pages'
    id = Column(Integer, primary_key = True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    page_type = Column(Integer) # see utils/common.py
    page_data = Column(MEDIUMTEXT)

class Post(Base):
    __tablename__ = 'posts'
    id = Column(String(32), primary_key = True, unique=True, autoincrement=False)	# reddit's id "id"
    subreddit_id = Column(String(32))	# "subreddit_id"
    created = Column(DateTime) # "created"
    #when this record was created:
    created_at = Column(DateTime, default=datetime.datetime.utcnow) 
    post_data = Column(MEDIUMTEXT)	# "json_dict"
    comment_data = Column(LONGTEXT)
    comments_queried_at = Column(DateTime, default=None)  

class ModAction(Base):
    __tablename__ = "mod_actions"
    id = Column(String(256), primary_key = True, unique=True, autoincrement=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)  
    created_utc = Column(DateTime)
    subreddit_id = Column(String(32), index=True)
    mod = Column(String(64))
    target_author = Column(String(64), index=True)
    action = Column(String(256))
    target_fullname = Column(String(256))
    action_data = Column(MEDIUMTEXT) # json_dict

class PrawKey(Base):
    __tablename__ = 'praw_keys'
    # IDs will be a string based on the assumption
    # that each device will only have one process
    # at a time handling a particular controller
    # in the format:  
    #   HOST:ENV:CONTROLLER
    # For example:
    #   hannahmore:development:FrontPageController
    id = Column(String(256), primary_key = True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    access_token = Column(String(256))
    scope = Column(String(256)) #json
    refresh_token = Column(String(256))

    @classmethod
    def get_praw_id(cls, env, controller):
        host = socket.gethostname()
        return "{0}:{1}:{2}".format(host,env,controller)
