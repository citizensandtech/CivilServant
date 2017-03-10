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

"""
    def archive_


    # returns the last action id, for paging purposes
    def archive_mod_action_page(self, after_id=None):
        if(after_id):
            self.log.info("Querying moderation log for {subreddit}, after_id = {after_id}".format(
                subreddit=self.subreddit, after_id = after_id))
        else:
            self.log.info("Querying moderation log for {subreddit}".format(subreddit=self.subreddit)) 

        actions = self.r.get_mod_log(self.subreddit, limit = 500, params={"after": after_id})
        action_count = 0
        last_action = None
        for action in actions:
            #### TO HANDLE TEST FIXTURES
            if("json_dict" in dir(action)):
                action = action.json_dict
            #### CREATE NEW OBJECT
            modaction = ModAction(
                id = action['id'],
                created_utc = datetime.datetime.fromtimestamp(action['created_utc']),
                subreddit_id = action['sr_id36'],
                mod = action['mod'],
                target_author = action['target_author'],
                action = action['action'],
                target_fullname = action['target_fullname'],
                action_data = json.dumps(action)   
            )
            try:
                self.db_session.add(modaction)
                self.db_session.commit()
            except (sqlalchemy.orm.exc.FlushError, sqlalchemy.exc.IntegrityError) as err:
                self.db_session.rollback()
                if("conflicts with" in err.__str__() or "Duplicate" in err.__str__()):
                    self.log.info("Some Moderator actions were already in the database. Not saving. Error: {}".format(err.__str__()))
                    print("Some Moderator actions were already in the database. Not saving.")
                else:
                    self.log.error(err.__str__())
            last_action = action
            action_count += 1

        self.log.info("Completed archive of {n} returned moderation actions for {subreddit}".format(
            n=action_count,
            subreddit = self.subreddit))

        return last_action['id']
"""