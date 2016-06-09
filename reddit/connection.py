import praw
import pickle
import os

def connect():
    env =  os.environ['CS_ENV']
    r = praw.Reddit(user_agent="Test version of CivilServant by u/natematias")
    with open("config/access_information_{environment}.pickle".format(environment=env), "rb") as fp:
        access_information = pickle.load(fp)
        r.set_access_credentials(**access_information)
    return r