import simplejson as json
import datetime
from app.models import Base, LumenNotice, LumenNoticeToTwitterUser
import utils.common
import requests
import app.controllers.twitter_controller
import sqlalchemy

class LumenController():
    def __init__(self, db_session, l, t, log):
        self.db_session = db_session
        self.l = l
        self.t = t
        self.log = log    

    # archives lumen notices since date til now(+1day)
    # if parse_for_users True, calls self.parse_notices_archive_users 
    def archive_lumen_notices(self, topics, date, parse_for_users=True):
        print("arhicivng")
        nowish = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        for topic in topics:
            next_page = 1
            while next_page is not None:

                payload = {
                    "topics": [topic],
                    "per_page": 50,
                    "page": next_page,
                    "sort_by": "date_received desc",
                    "recipient_name": "Twitter",
                    "date_received_facet": {
                        "from": utils.common.time_since_epoch_ms(date),
                        "to": utils.common.time_since_epoch_ms(nowish)
                    }
                }

                data = self.l.get_search(payload)
                notices_json = data["notices"]
                next_page = data["meta"]["next_page"]

                added_notices = []
                for notice in notices_json:
                    queried_notice = self.db_session.query(LumenNotice).filter(LumenNotice.id == notice["id"]).first()
                    if not queried_notice:
                        sender = (notice["sender_name"].encode("utf-8", "replace") if notice["sender_name"] else "")
                        principal = (notice["principal_name"].encode("utf-8", "replace") if notice["principal_name"] else "")
                        recipient = (notice["recipient_name"].encode("utf-8", "replace") if notice["recipient_name"] else "")
                        num_infringing_urls = len(notice["works"][0]["infringing_urls"]) if len(notice["works"]) > 0 else 0
                        notice_record = LumenNotice(
                            id = notice["id"],
                            date_received = datetime.datetime.strptime(notice["date_received"], '%Y-%m-%dT%H:%M:%S.000Z'),  # expect string like "2017-04-15T22:28:26.000Z"
                            sender = sender,
                            principal = principal,
                            recipient = recipient,
                            num_infringing_urls = num_infringing_urls,
                            notice_data = json.dumps(notice).encode("utf-8", "replace")
                        )
                        self.db_session.add(notice_record)
                        added_notices.append(notice)
                try:
                    self.db_session.commit()
                    self.log.info("Saved {0} lumen notices.".format(len(added_notices)))
                except:         
                    self.log.error("Error while saving DB Session")

                if parse_for_users: # this boolean is for unit testing purposes
                    self.parse_notices_archive_users(added_notices)

    # expecting ~50 notices
    # if archive_users true, also calls TwitterController (boolean exists for testing purposes)
    def parse_notices_archive_users(self, notices, archive_users=True):
        for notice in notices:
            users = set([])
            for work in notice["works"]:
                # infringing_urls is known to contain urls
                for url_obj in work["infringing_urls"]:
                    url = url_obj["url"]
                    username = helper_parse_url_for_username(url)    
                    if username:
                        users.add(username)
                if notice["body"]:  # I've only seen this null
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice['body']; notice id = {0}".format(notice["id"]))                
                if len(work["copyrighted_urls"]) > 0:  # I've only seen this empty
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice['works']['copyrighted_urls']; notice id = {0}".format(notice["id"]))                
                if work["description"]:  # I've only seen this null
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice['works']['description']; notice id = {0}".format(notice["id"]))                

            for username in users:
                notice_user_record = LumenNoticeToTwitterUser(
                        notice_id = notice["id"],
                        twitter_username = username)
                self.db_session.add(notice_user_record)
            try:
                self.db_session.commit()
                self.log.info("Saved {0} users.".format(len(users)))
            except:
                self.log.error("Error while saving DB Session")

        # calls the twitter controller
        if archive_users:
            self.t.archive_users(users_to_notices.keys())


# assume url is of the form 'https://twitter.com/sooos243/status/852942353321140224' 
# OR check if a t.co url extends to a twitter.com url 
# interesting later study: see how many t.co links resolve to twitter links?
def helper_parse_url_for_username(url):
    twitter_domain = "twitter.com"
    tco_domain = "t.co"
    username = None
    url_split = url.split("/")

    if len(url_split) >= 3 and url_split[2] == tco_domain:
        # try to get request and unshorten the url
        try:
            r = requests.get(url)
            if r:
                url = r.url
                url_split = url.split("/")
            else:
                raise Exception
        except:
            return None

    if url == "https://twitter.com/account/suspended":
        # TODO: then we have no information. what should we do about them? should we count these? 
        return None

    if len(url_split) >= 3 and url_split[2] == twitter_domain:
        username = url_split[3]
    return username