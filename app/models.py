# example taken from http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, Text, DateTime, String
from sqlalchemy.dialects.mysql import MEDIUMTEXT
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
    page_data = Column(MEDIUMTEXT)


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