import praw
import pickle
import os

def connect():
    env =  os.environ['CS_ENVIRONMENT']
    r = praw.Reddit(user_agent="Test version of CivilServant by u/natematias")
    access_information = pickle.load(open("config/access_information_{environment}.pickle".format(environment=env), "rb"))
    r.set_access_credentials(**access_information)
    return r