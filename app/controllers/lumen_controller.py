import simplejson as json
import datetime
from app.models import Base, LumenNotice, LumenNoticeToTwitterUser, TwitterUser
from app.controllers.twitter_controller import TwitterController
import utils.common
from utils.common import CS_JobState
import requests
import app.controllers.twitter_controller
import sqlalchemy
import sys

class LumenController():
    def __init__(self, db_session, l, log):
        self.db_session = db_session
        self.l = l
        self.log = log    

    # archives lumen notices since date til now(+1day)
    def archive_lumen_notices(self, topics, date):
        nowish = datetime.datetime.utcnow() + datetime.timedelta(days=1)

        recent_notices = self.db_session.query(LumenNotice).filter(LumenNotice.date_received >= date).all()
        recent_notices_ids = set([notice.id for notice in recent_notices])

        for topic in topics:
            next_page = 1
            while next_page is not None:
                data = self.l.get_notices_to_twitter([topic], 50, next_page, date, nowish)
                
                #with open("tests/fixture_data/lumen_notices_0.json") as f:
                #    data = json.loads(f.read())
                
                if not data:
                    # error is already logged by get_notices_to_twitter
                    return 

                notices_json = data["notices"]
                next_page = data["meta"]["next_page"]


                added_notices_ids = set([])
                prev_add_notices_size = len(added_notices_ids)
                for notice in notices_json:
                    nid = notice["id"]
                    date_received = datetime.datetime.strptime(notice["date_received"], '%Y-%m-%dT%H:%M:%S.000Z') # expect string like "2017-04-15T22:28:26.000Z"
                    if nid not in recent_notices_ids and date_received >= date and date_received <= nowish:
                        try:
                            sender = (notice["sender_name"].encode("utf-8", "replace") if notice["sender_name"] else "")
                            principal = (notice["principal_name"].encode("utf-8", "replace") if notice["principal_name"] else "")
                            recipient = (notice["recipient_name"].encode("utf-8", "replace") if notice["recipient_name"] else "")
                            notice_record = LumenNotice(
                                id = nid,
                                record_created_at = datetime.datetime.utcnow(),
                                date_received = date_received,
                                sender = sender,
                                principal = principal,
                                recipient = recipient,
                                notice_data = json.dumps(notice).encode("utf-8", "replace"),
                                CS_parsed_usernames = CS_JobState.NOT_PROCESSED.value)
                            self.db_session.add(notice_record)
                            recent_notices_ids.add(nid)
                            added_notices_ids.add(nid)
                        except:
                            self.log.error("Error while creating LumenNotice object for notice {0}".format(notice["id"]), extra=sys.exc_info()[0])
                if len(added_notices_ids) == prev_add_notices_size:
                    break

                prev_add_notices_size = len(added_notices_ids)
                try:
                    self.db_session.commit()
                except:         
                    self.log.error("Error while saving {0} lumen notices in DB Session".format(len(added_notices_ids)), extra=sys.exc_info()[0])
                else:
                    self.log.info("Saved {0} lumen notices.".format(len(added_notices_ids)))


    """
    For all LumenNotices with CS_parsed_usernames=NOT_PROCESSED, parse for twitter accounts
    """
    def query_and_parse_notices_archive_users(self):
        unparsed_notices = self.db_session.query(LumenNotice).filter(LumenNotice.CS_parsed_usernames == CS_JobState.NOT_PROCESSED.value).all()

        utils.common.update_CS_JobState(unparsed_notices, "CS_parsed_usernames", CS_JobState.IN_PROGRESS, self.db_session, self.log)

        notice_to_state = self.parse_notices_archive_users(unparsed_notices)

        utils.common.update_all_CS_JobState(notice_to_state, "CS_parsed_usernames", self.db_session, self.log)


    """
        unparsed_notices = list of LumenNotices

        returns:
            notice_to_state = {LumenNotice: CS_JobState}
    """
    def parse_notices_archive_users(self, unparsed_notices):
        if len(unparsed_notices) == 0:
            return {}

        is_test = type(unparsed_notices[0]) is not LumenNotice
        if not is_test: # to accomodate test fixture data
            notice_to_state = {notice: CS_JobState.FAILED for notice in unparsed_notices }
        else:
            notice_to_state = {json.dumps(notice): CS_JobState.FAILED for notice in unparsed_notices } 

        for notice in unparsed_notices:
            notice_json = json.loads(notice.notice_data) if not is_test else notice # to accomodate test fixture data
            notice_users = set([])
            suspended_user_count = 0
            job_state = None
            for work in notice_json["works"]:
                # infringing_urls is known to contain urls
                for url_obj in work["infringing_urls"]:
                    url = url_obj["url"]
                    try:
                        username = helper_parse_url_for_username(url, self.log)    
                    except utils.common.ParseUsernameSuspendedUserFound:
                        suspended_user_count += 1
                    except Exception as e:
                        self.log.error("Unexpected error while calling helper_parse_url_for_username on url {0}: {1}".format(url, e))
                    else:
                        if username:
                            # if no username, then no username found
                            notice_users.add(username)
                        
                if len(work["copyrighted_urls"]) > 0:  # I've only seen this empty
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice_json['works']['copyrighted_urls']; notice id = {0}".format(notice_json["id"]))                
                    job_state = CS_JobState.NEEDS_RETRY
                if work["description"]:  # I've only seen this null
                    self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice_json['works']['description']; notice id = {0}".format(notice_json["id"]))                
                    job_state = CS_JobState.NEEDS_RETRY
            if notice_json["body"]:  # I've only seen this null
                self.log.error("method helper_parse_notices_archive_users: maybe missed something in notice_json['body']; notice id = {0}".format(notice_json["id"]))                
                job_state = CS_JobState.NEEDS_RETRY

            now = datetime.datetime.utcnow()
            # for every notice, commit LumenNoticeToTwitterUser records 
            for username in notice_users:
                notice_user_record = LumenNoticeToTwitterUser(
                        record_created_at = now,
                        notice_id = notice_json["id"],
                        twitter_username = username.lower(),
                        twitter_user_id = None,
                        CS_account_archived = CS_JobState.NOT_PROCESSED.value
                    )
                self.db_session.add(notice_user_record)

            # this notice has suspended_user_count not found users (t.co URL redirected to an account/suspended page)
            for i in range(suspended_user_count):
                notice_user_record = LumenNoticeToTwitterUser(
                        record_created_at = now,
                        notice_id = notice_json["id"],
                        twitter_username = utils.common.NOT_FOUND_TWITTER_USER_STR,
                        twitter_user_id = utils.common.NOT_FOUND_TWITTER_USER_STR,
                        CS_account_archived = CS_JobState.PROCESSED.value # can't do anything about these records. don't process
                    )
                self.db_session.add(notice_user_record)                

            try:
                self.db_session.commit()
            except:
                self.log.error("Error while saving {0} twitter users from {1} infringing_urls in notice {2} DB Session".format(
                    len(notice_users),
                    sum(len(work["infringing_urls"]) for work in notice_json["works"]),
                    notice_json["id"]), extra=sys.exc_info()[0])
                ####return notice_to_state
            else:
                self.log.info("Saved {0} twitter users from {1} infringing_urls in notice {2}.".format(
                    len(notice_users),
                    sum(len(work["infringing_urls"]) for work in notice_json["works"]),
                    notice_json["id"]))

                job_state = CS_JobState.PROCESSED if (job_state is not CS_JobState.NEEDS_RETRY) else CS_JobState.NEEDS_RETRY
                key = notice if not is_test else json.dumps(notice)
                notice_to_state[key] = job_state

        return notice_to_state


# assume url is of the form 'https://twitter.com/sooos243/status/852942353321140224' 
# OR check if a t.co url extends to a twitter.com url 
# interesting later study: see how many t.co links resolve to twitter links?
def helper_parse_url_for_username(url, log):
    twitter_domain = "twitter.com"
    tco_domain = "t.co"
    username = None
    url_split = url.split("/")
    retries = 3

    # TODO: how to resolve t.co urls without hitting twitter.com without auth tokens (since we're getting rate limited?) 
    # calling requests.get is very time inefficient
    if len(url_split) >= 3 and url_split[2] == tco_domain:
        log.error("t.co url that we didn't attempt to resolve: {0}".format(url))
        # try to get request and unshorten the url

        #####r = None
        #####while retries > 0:
        #####    try:
        #####        r = requests.get(url)
        #####        url = r.url
        #####        url_split = url.split("/")
        #####    except:
        #####        retries -=1
        #####if retries == 0 and not r:
        #####    raise Exception

    if url == "https://twitter.com/account/suspended":
        # TODO: then we have no information. 
        # save a LumenNoticeToTwitterUser record, with username = "SUSPENDED"
        raise utils.common.ParseUsernameSuspendedUserFound

    if len(url_split) >= 3 and url_split[2] == twitter_domain:
        username = url_split[3]
        
    return username
