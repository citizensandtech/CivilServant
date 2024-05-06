#!/usr/bin/env python3

## THIS SCRIPT GATHERS ACCESS INFORMATION
## INCLUDING A REFRESH KEY FROM REDDIT

# DOCUMENTATION FROM:
# http://praw.readthedocs.io/en/stable/pages/oauth.html

import praw
import pickle
import os

env =  os.environ['CS_ENV']

r = praw.Reddit(user_agent="Test version of CivilServant by u/natematias")
scopes = ["identity", "read", "modlog", "modposts", "submit", "modconfig", "flair", "privatemessages"]
url = r.auth.url(scopes=scopes, state="uniqueKey", duration="permanent")

print("Please visit the following URL and click Allow:")
print(url)
print("After you give your permission, please enter the code from the redirect_url.")
access_token = input("Enter the text after 'code=':\n")
access_token = access_token.replace("#_", "")
refresh_token = r.auth.authorize(access_token)

with open("config/access_information_{environment}.pickle".format(environment=env), "wb") as f:
    access_information = {
        'access_token': access_token,
        'refresh_token': refresh_token,
        'scope': r.auth.scopes()
    }
    pickle.dump(access_information, f)

print("config/access_information_{environment}.pickle created\n".format(environment=env))
