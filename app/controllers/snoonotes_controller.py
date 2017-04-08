import inspect, os, sys, copy, pytz, re, glob

#import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import sqlalchemy
import requests
from utils.common import PageType
from app.models import Base, SnooNote
from sqlalchemy import and_

class SnooNotesController:
    def __init__(self, subreddit, db_session, s, log):
        self.subreddit = subreddit
        self.db_session = db_session
        self.log = log
        self.s = s

    ## FETCH ALL USERS ACCESSIBLE TO THIS ACCOUNT
    def fetch_all_users(self):
        res_json = s.get_users()
        if res_json not None:
            #TODO: archive usernames_with_notes

    ## FETCH ALL NOTES FOR MENTIONED USERS
    def fetch_all_notes(self, users):
        all_user_notes = []

        #offset = 0 # ????
        # payload = users[0+offset:1000 + offset] #TODO: this line is broken???

        res_json = s.post_get_notes(payload) v
        if res_json not None:
            #TODO: archive notes....

            """
            # write usernotes to file
            with open('outputs/snoonotes_02.13.2017-10.00.json', 'w') as fp:
                json.dump(all_notes, fp)
            """


    ## Fetch User Notes Schemas
    def fetch_user_notes_schemas(self):
        res_json = s.get_schemas()
        if res_json not None:
            #TODO: archive usernotes...

            """
            # write usernotes to file
            with open('outputs/snoonotes_schema_02.13.2017-10.00.json', 'w') as fp:
                json.dump(schema, fp)
            """