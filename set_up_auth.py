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
import urllib

from prawcore import (Authorizer, ImplicitAuthorizer, TrustedAuthenticator, UntrustedAuthenticator, session)
from utils.common

env =  os.environ['CS_ENV']

r = praw.Reddit()

username = r.config.username

auth = praw.models.Auth(r, _data=None)
scope = ['identity','read','modlog','modposts','submit','modconfig','flair']
url = urllib.parse.unquote(auth.url(scope, 'uniqueKey',duration='permanent'))
print(url)
print("After you accept permission, please enter the code from the redirect_url")
code = input("Enter the text after 'code='\n")

r.config.username = None # hacky - just to let us use authorize
refresh_token = auth.authorize(code)
access_token = auth._core._authorizer.access_token # hacky

access_information = {
	"access_token": access_token,
	"refresh_token": refresh_token,
	"scope": scope,
	"username": username # adding it back to r.config
}

utils.common.set_access_credentials(r, **access_information)
# TODO: anything to do here??

pickle.dump(access_information, open("config/access_information_praw4_{environment}.pickle".format(environment=env), "wb"))
print( "config/access_information_praw4_{environment}.pickle created".format(environment=env) )
print()