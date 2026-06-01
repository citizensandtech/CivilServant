import praw
import os
from app.models import PrawKey
from utils.common import DbEngine

ENV = os.environ['CS_ENV']

class Connect:

  def __init__(self, db_session=None, base_dir="", env=None):
    self.base_dir = base_dir
    self.env = env if env else ENV

    if db_session is None:
      self.db_session = DbEngine(os.path.join(self.base_dir, "config", "{env}.json".format(env=self.env))).new_session()
    else:
      self.db_session = db_session

  def connect(self, controller="Main"):
    # PRAW 7 authenticates at construction from a stored refresh token.
    # The OAuth app credentials come from praw.ini's [DEFAULT] section.
    # Fall back to the shared "Main" row that set_up_auth.py seeds.
    # The refresh token is account-wide, so one authorization serves every controller.
    pk = self._praw_key(controller)
    if pk is None and controller != "Main":
      pk = self._praw_key("Main")
    if pk is None:
      raise RuntimeError(
        "No PrawKey found for env '{0}' (controller '{1}' or 'Main'). "
        "Run set_up_auth.py to authorize this account.".format(self.env, controller))
    return praw.Reddit(refresh_token=pk.refresh_token)

  def _praw_key(self, controller):
    praw_id = PrawKey.get_praw_id(self.env, controller)
    return self.db_session.query(PrawKey).filter_by(id=praw_id).first()
