"""
Perform OAuth code flow and store the refresh token.

Usage: CS_ENV=<env> PYTHONPATH=. python3 set_up_auth.py [controller]
"""

import os
import sys
import praw
import simplejson as json

from app.models import PrawKey
from utils.common import DbEngine

ENV = os.environ["CS_ENV"]

SCOPES = [
    "identity",
    "read",
    "modlog",
    "modposts",
    "submit",
    "modconfig",
    "flair",
    "privatemessages",
]


def main(controller="Main"):
    base_dir = os.path.dirname(os.path.realpath(__file__))
    db_session = DbEngine(
        os.path.join(base_dir, "config", "{env}.json".format(env=ENV))
    ).new_session()

    reddit = praw.Reddit()

    url = reddit.auth.url(scopes=SCOPES, state="uniqueKey", duration="permanent")
    print(
        "Visit this URL, grant access, then copy the 'code' parameter from the redirect URL:"
    )
    print(url)
    code = input("Enter the text after 'code='\n").strip()

    refresh_token = reddit.auth.authorize(code)

    me = reddit.user.me()
    praw_id = PrawKey.get_praw_id(ENV, controller)
    scope_json = json.dumps(list(reddit.auth.scopes()))

    existing = db_session.query(PrawKey).filter_by(id=praw_id).first()
    if existing:
        existing.refresh_token = refresh_token
        existing.scope = scope_json
        existing.authorized_username = me.name
        existing.authorized_user_id = me.id
    else:
        db_session.add(
            PrawKey(
                id=praw_id,
                refresh_token=refresh_token,
                scope=scope_json,
                authorized_username=me.name,
                authorized_user_id=me.id,
            )
        )
    db_session.commit()
    print(
        "Stored refresh token in PrawKey row '{0}' (authorized as u/{1}).".format(
            praw_id, me.name
        )
    )


if __name__ == "__main__":
    requested_controller = sys.argv[1] if len(sys.argv) > 1 else "Main"
    main(requested_controller)
