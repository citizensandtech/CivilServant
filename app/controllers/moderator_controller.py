import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
import sqlalchemy
from utils.common import PageType
from app.models import Base, SubredditPage, Subreddit, Post, ModAction
from sqlalchemy import and_

class ModeratorController:
    def __init__(self, subreddit, db_session, r, log):
        self.subreddit = subreddit
        self.db_session = db_session
        self.log = log
        self.r = r 

    # returns the last action id, for paging purposes
    def archive_mod_action_page(self, after_id=None):
        if(after_id):
            self.log.info("Querying moderation log for {subreddit}, after_id = {after_id}".format(
                subreddit=self.subreddit, after_id = after_id))
        else:
            self.log.info("Querying moderation log for {subreddit}".format(subreddit=self.subreddit)) 

        actions = self.r.get_mod_log(self.subreddit, limit = 500, params={"after": after_id})
        action_dicts = []
        for action in actions:
            if("json_dict" in dir(action)):
                action = action.json_dict # to handle test fixtures
            action_dicts.append(dict(
                id = action['id'],
                created_utc = datetime.datetime.fromtimestamp(action['created_utc']),
                subreddit_id = action['sr_id36'],
                mod = action['mod'],
                target_author = action['target_author'],
                action = action['action'],
                target_fullname = action['target_fullname'],
                action_data = json.dumps(action)   
            ))
        
        try:
            result = self.db_session.insert_retryable(ModAction, action_dicts)
        except:
            log.exception("An error occurred while trying to bulk insert moderation actions for {subreddit}".format(
                subreddit = self.subreddit))
            raise
            
        self.log.info("Completed archive of {unique_n} unique moderation actions of {returned_n} returned moderation actions for {subreddit}".format(
            unique_n=result.rowcount,
            returned_n=len(action_dicts),
            subreddit = self.subreddit))

        return action_dicts[-1]['id'], result.rowcount
