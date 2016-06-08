# example taken from http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, Text, DateTime
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
    page_data = Column(MEDIUMTEXT)
