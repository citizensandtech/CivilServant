import simplejson as json
import datetime
from app.models import Base, LumenNotice, LumenNoticeToTwitterUser
from app.controllers.twitter_controller import TwitterController
import utils.common
import requests
import app.controllers.twitter_controller
import sqlalchemy

class LumenController():
    def __init__(self, db_session, l, t, log):
        self.db_session = db_session
        self.l = l
        self.t = t # TwitterConnect
        self.log = log    

        self.tc = TwitterController(self.db_session, self.t, self.log) # TwitterController


    # archives lumen notices since date til now(+1day)
    # if parse_for_users True, calls self.parse_notices_archive_users 
    def archive_lumen_notices(self, topics, date, parse_for_users=True):
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
                #with open("tests/fixture_data/lumen_notices_0.json") as f:
                #    data = json.loads(f.read())
                notices_json = data["notices"]
                next_page = data["meta"]["next_page"]

                added_notices = []
                for notice in notices_json:
                    if not self.db_session.query(LumenNotice).filter(LumenNotice.id == notice["id"]).first():
                        try:
                            date_received = datetime.datetime.strptime(notice["date_received"], '%Y-%m-%dT%H:%M:%S.000Z') # expect string like "2017-04-15T22:28:26.000Z"
                            sender = (notice["sender_name"].encode("utf-8", "replace") if notice["sender_name"] else "")
                            principal = (notice["principal_name"].encode("utf-8", "replace") if notice["principal_name"] else "")
                            recipient = (notice["recipient_name"].encode("utf-8", "replace") if notice["recipient_name"] else "")
                            num_infringing_urls = sum(len(work["infringing_urls"]) for work in notice["works"])
                            self.log.info(num_infringing_urls)
                            notice_record = LumenNotice(
                                id = notice["id"],
                                date_received = date_received,
                                sender = sender,
                                principal = principal,
                                recipient = recipient,
                                num_infringing_urls = num_infringing_urls,
                                notice_data = json.dumps(notice).encode("utf-8", "replace"))
                            self.db_session.add(notice_record)
                            self.log.info("added {0}".format(notice["id"]))
                            added_notices.append(notice)
                        except:
                            self.log.error("Error while creating LumenNotice object for notice {0}".format(notice["id"]))
                try:
                    self.db_session.commit()
                    self.log.info("Saved {0} lumen notices.".format(len(added_notices)))
                except:         
                    self.log.error("Error while saving DB Session")

                if parse_for_users: # this boolean is for unit testing purposes
                    self.parse_notices_archive_users(added_notices)

    # if archive_users true, also calls TwitterController (boolean exists for testing purposes)
    def parse_notices_archive_users(self, notices, archive_users=True):
        all_users = set([])
        for notice in notices:  # expecting ~50 notices = several hundred users
            notice_users = set([])
            for work in notice["works"]:
                # infringing_urls is known to contain urls
                for url_obj in work["infringing_urls"]:
                    url = url_obj["url"]
                    username = helper_parse_url_for_username(url)    
                    if username:
                        notice_users.add(username)
                        all_users.add(username)
                if notice["body"]:  # I've only seen this null
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice['body']; notice id = {0}".format(notice["id"]))                
                if len(work["copyrighted_urls"]) > 0:  # I've only seen this empty
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice['works']['copyrighted_urls']; notice id = {0}".format(notice["id"]))                
                if work["description"]:  # I've only seen this null
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice['works']['description']; notice id = {0}".format(notice["id"]))                

            # for every notice, commit LumenNoticeToTwitterUser records 
            for username in notice_users:
                notice_user_record = LumenNoticeToTwitterUser(
                        notice_id = notice["id"],
                        twitter_username = username.lower())
                self.db_session.add(notice_user_record)
            try:
                self.db_session.commit()
                self.log.info("Saved {0} twitter users from {1} infringing_urls in notice {2}.".format(
                    len(notice_users), 
                    sum(len(work["infringing_urls"]) for work in notice["works"]),
                    notice["id"]))
            except:
                self.log.error("Error while saving DB Session")

        # for every batch of ~50 notices, calls the twitter controller. 
        # self.tc.archive_users is most efficient when you have >100 users
        if archive_users:
            self.tc.archive_users(all_users)


# assume url is of the form 'https://twitter.com/sooos243/status/852942353321140224' 
# OR check if a t.co url extends to a twitter.com url 
# interesting later study: see how many t.co links resolve to twitter links?
def helper_parse_url_for_username(url):
    twitter_domain = "twitter.com"
    tco_domain = "t.co"
    username = None
    url_split = url.split("/")

    # calling requests.get is very time inefficient
    #"""
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
    #"""

    if url == "https://twitter.com/account/suspended":
        # TODO: then we have no information. what should we do about them? should we count these? 
        return None

    if len(url_split) >= 3 and url_split[2] == twitter_domain:
        username = url_split[3]
    return username
