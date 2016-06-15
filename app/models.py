# example taken from http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, Text, DateTime, Enum
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
import sqlalchemy
import datetime

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
    subreddit_id = Column(String(32), ForeignKey('subreddits.id'))	# "subreddit_id"
    created = Column(DateTime) # "created"
    post_data = Column(MEDIUMTEXT)	# "json_dict"