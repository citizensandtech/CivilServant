## THIS SCRIPT GATHERS ACCESS INFORMATION
## INCLUDING A REFRESH KEY FROM REDDIT

# DOCUMENTATION FROM:
# http://praw.readthedocs.io/en/stable/pages/oauth.html

import praw
import webbrowser
import pickle
import sys
import simplejson as json
import os

env =  os.environ['CS_ENV']

r = praw.Reddit(user_agent="Test version of CivilServant by u/natematias")

url = r.get_authorize_url('uniqueKey', 'identity read modlog modposts submit modconfig flair', True)
print(url)
print("After you accept permission, please enter the code from the redirect_url")
code = input("Enter the text after 'code='\n")
access_information = r.get_access_information(code)
pickle.dump(access_information, open("config/access_information_{environment}.pickle".format(environment=env), "wb"))
#r.set_access_credentials(**access_information)
#print code
#print access_information

print( "config/access_information_{environment}.pickle created".format(environment=env) )
print
