import praw
import os
import simplejson as json
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
    # PRAW 7 authenticates at construction from a stored refresh token, which is account-wide.
    # The OAuth app credentials come from praw.ini's [DEFAULT] section.
    # Fall back to the shared "Main" row that set_up_auth.py seeds.
    pk = self._praw_key(controller)
    if pk is None and controller != "Main":
      pk = self._praw_key("Main")
    # Fall back to `config/<env>_auth.json`
    refresh_token = pk.refresh_token if pk is not None else self._refresh_token_from_file()
    if refresh_token is None:
      raise RuntimeError(
        "No PrawKey for env '{0}' (controller '{1}' or 'Main') and no '{2}' file. Run set_up_auth.py.".format(self.env, controller, self._auth_file_path()))
    return praw.Reddit(refresh_token=refresh_token)

  def _praw_key(self, controller):
    praw_id = PrawKey.get_praw_id(self.env, controller)
    return self.db_session.query(PrawKey).filter_by(id=praw_id).first()

  def _auth_file_path(self):
    return os.path.join(self.base_dir, "config", "{env}_auth.json".format(env=self.env))

  def _refresh_token_from_file(self):
    path = self._auth_file_path()
    if not os.path.exists(path):
      return None
    with open(path) as auth_file:
      return json.load(auth_file).get("refresh_token")
