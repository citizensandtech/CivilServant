import os
import praw
import pickle

env =  os.environ['CS_ENVIRONMENT']

r = praw.Reddit(user_agent="Test version of CivilServant by u/natematias")
access_information = pickle.load(open("config/access_information_{environment}.pickle".format(environment=env), "rb"))
r.set_access_credentials(**access_information)

posts = []
sub = r.get_subreddit("science")
for post in sub.get_new(limit=100):
  posts.append(post)
  print(post.__dict__)
  print
