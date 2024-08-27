import praw
import inspect, os, sys  # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
import sqlalchemy
import app.event_handler
from utils.common import PageType
from app.models import Base, SubredditPage, Subreddit, Post, ModAction
from sqlalchemy import and_


class ModeratorController:
    def __init__(self, subreddit, db_session, r, log):
        self.subreddit = subreddit
        self.db_session = db_session
        self.log = log
        self.r = r
        self.fetched_mod_actions = []
        self.fetched_subreddit_id = None

    # returns the last action id, for paging purposes
    @app.event_handler.event_handler
    def archive_mod_action_page(self, after_id=None):
        if after_id:
            self.log.info(
                "Querying moderation log for {subreddit}, after_id = {after_id}".format(
                    subreddit=self.subreddit, after_id=after_id
                )
            )
        else:
            self.log.info(
                "Querying moderation log for {subreddit}".format(
                    subreddit=self.subreddit
                )
            )

        self.fetched_mod_actions = list(
            self.r.get_mod_log(self.subreddit, limit=500, params={"after": after_id})
        )
        if self.fetched_mod_actions:
            first_ma = self.fetched_mod_actions[0]
            src_dict = getattr(first_ma, "json_dict", first_ma)
            self.fetched_subreddit_id = src_dict["sr_id36"]

        action_dicts = []
        for action in self.fetched_mod_actions:
            if "json_dict" in dir(action):
                action = action.json_dict  # to handle test fixtures
            action_dicts.append(
                dict(
                    id=action["id"],
                    created_utc=datetime.datetime.fromtimestamp(action["created_utc"]),
                    subreddit_id=action["sr_id36"],
                    mod=action["mod"],
                    target_author=action["target_author"],
                    action=action["action"],
                    target_fullname=action["target_fullname"],
                    action_data=json.dumps(action),
                )
            )

        if len(action_dicts) == 0:
            unique_count = 0
        else:
            try:
                result = self.db_session.insert_retryable(ModAction, action_dicts)
                unique_count = result.rowcount
            except:
                self.log.exception(
                    "An error occurred while trying to bulk insert moderation actions for {subreddit}".format(
                        subreddit=self.subreddit
                    )
                )
                raise

        self.log.info(
            "Completed archive of {unique_count} unique moderation actions of {returned_count} returned moderation actions for {subreddit}".format(
                unique_count=unique_count,
                returned_count=len(action_dicts),
                subreddit=self.subreddit,
            )
        )

        last_id = action_dicts[-1]["id"] if len(action_dicts) > 0 else None
        return last_id, unique_count
