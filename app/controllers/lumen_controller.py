import simplejson as json
import datetime
from app.models import Base, LumenNotice, LumenNoticeToTwitterUser, TwitterUser, LumenNoticeExpandedURL
from app.controllers.twitter_controller import TwitterController
import utils.common
from utils.common import CS_JobState
import requests
from requests_futures.sessions import FuturesSession
from concurrent.futures import wait
import app.controllers.twitter_controller
import sqlalchemy
from sqlalchemy import or_
import sys
import time
import warnings

class LumenController():
    def __init__(self, db_session, l, log):
        self.db_session = db_session
        self.l = l
        self.log = log

    # archives lumen notices since date til now(+1day)
    def archive_lumen_notices(self, topics, date):
        nowish = datetime.datetime.utcnow() + datetime.timedelta(days=1)

        # notices already stored in db
        added_notices = self.db_session.query(LumenNotice).filter(LumenNotice.date_received >= date).all()
        added_notices_ids = set([notice.id for notice in added_notices])
        newly_added_notices_ids = set([])

        for topic in topics:
            next_page = 1
            while next_page is not None:
                # sleep for 2 seconds if we're calling page 2 or more
                # for now, implement this here. In future add to connection library
                if(next_page > 1):
                  time.sleep(2)


                data = self.l.get_notices_to_twitter([topic], 50, next_page, date, nowish)
                if not data:
                    # error is already logged by get_notices_to_twitter
                    return

                notices_json = data["notices"]
                self.log.debug('next_page of pagination has value {}'.format(next_page))
                self.log.debug('{} notices returned from Lumen Call'.format(len(notices_json)))
                # self.log.debug('lumen meta response is: {}'.format(data['meta']))
                # next_page = data["meta"]["next_page"]
                ## Danger hack because Lumen is not returning next_page properly.
                next_page = next_page + 1 if next_page <= data['meta']['total_pages'] else None
                max_date_received = None

                prev_add_notices_size = len(newly_added_notices_ids)
                for notice in notices_json:
                    nid = notice["id"]
                    date_received = datetime.datetime.strptime(notice["date_received"], '%Y-%m-%dT%H:%M:%S.000Z') # expect string like "2017-04-15T22:28:26.000Z"
                    max_date_received = max(date_received, max_date_received) if max_date_received else date_received
                    if nid not in added_notices_ids and nid not in newly_added_notices_ids and date_received >= date and date_received <= nowish:
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
                            newly_added_notices_ids.add(nid)
                        except:
                            self.log.error("Error while creating LumenNotice object for notice {0}".format(notice["id"]), extra=sys.exc_info()[0])

                try:
                    self.db_session.commit()
                except:
                    self.log.error("Error while saving {0} lumen notices in DB Session".format(len(added_notices_ids)), extra=sys.exc_info()[0])
                else:
                    self.log.info("Saved {0} lumen notices.".format(len(newly_added_notices_ids) - prev_add_notices_size))

                if next_page and next_page > 4 and len(newly_added_notices_ids) == prev_add_notices_size and max_date_received <= nowish:
                    # if we got lumen notices that are from at most nowish and we have seen them all before,
                    # hacky: always look at at least 3 pages
                    break

        self.log.info("fetch_lumen_notices saved {0} total new lumen notices.".format(len(newly_added_notices_ids)))



    def query_and_parse_notices_archive_users(self, test_exception = False):
        """
        For all LumenNotices with CS_parsed_usernames=NOT_PROCESSED, parse for twitter accounts
        """
        unparsed_notices = self.db_session.query(LumenNotice).filter(LumenNotice.CS_parsed_usernames == CS_JobState.NOT_PROCESSED.value).all()
        self.parse_notices_archive_users(unparsed_notices, test_exception)

    def parse_notices_archive_users(self, unparsed_notices, test_exception = False):
        """
            unparsed_notices = list of LumenNotices
        """
        if len(unparsed_notices) == 0:
            return {}

        is_test = type(unparsed_notices[0]) is not LumenNotice

        if(test_exception):
            counter = 0

        for notice in unparsed_notices:
            notice_old_job_state = notice.CS_parsed_usernames
            notice.CS_parsed_usernames = CS_JobState.IN_PROGRESS.value
            self.db_session.add(notice)
            self.db_session.commit()

            try:
                notice_json = json.loads(notice.notice_data) if not is_test else notice # to accomodate test fixture data
                notice_users = set([])
                suspended_user_count = 0
                job_state = None
                for work in notice_json["works"]:
                    # infringing_urls is known to contain urls

                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
                        unshortened_urls = self.bulk_unshorten(notice.id, [x['url'] for x in work['infringing_urls']])
                    infringing_urls = []
                    for url_dict in unshortened_urls.values():
                        if(url_dict['final_url'] is not None):
                            infringing_urls.append({"url":url_dict['final_url'],
                                                    "url_original":url_dict['original_url']})

                    for url_obj in infringing_urls:
                        url = url_obj["url"]
                        if url:
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
                        else:
                            self.log.info('There was no url for url_obj: {0}'.format(url_obj))

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

                if(test_exception):
                    counter += 1
                    if counter >= len(unparsed_notices) / 2:
                        raise Exception("Throwing an exception for test purposes")

            except Exception as e:
                # something unhandled went wrong during processing
                self.log.error('Lumen error while parsing is: {}'.format(e))
                notice.CS_parsed_usernames = notice_old_job_state
                self.db_session.add(notice)
                self.db_session.commit()
                raise # re-raise exception
            else:
                # commit previously added LumenNoticeToTwitterUser record
                # add & commit LumenNotice job state
                # finish processing
                try:
                    job_state = CS_JobState.PROCESSED.value if (job_state is not CS_JobState.NEEDS_RETRY.value) else CS_JobState.NEEDS_RETRY.value
                    notice.CS_parsed_usernames = job_state
                    self.db_session.add(notice)
                    self.db_session.commit()
                except:
                    self.log.error("Error while saving {0} twitter users from {1} infringing_urls in notice {2} DB Session".format(
                        len(notice_users),
                        sum(len(work["infringing_urls"]) for work in notice_json["works"]),
                        notice_json["id"]), extra=sys.exc_info()[0])
                else:
                    self.log.info("Saved {0} twitter users from {1} infringing_urls in notice {2}.".format(
                        len(notice_users),
                        sum(len(work["infringing_urls"]) for work in notice_json["works"]),
                        notice_json["id"]))

                    key = notice if not is_test else json.dumps(notice)

    def bulk_unshorten(self,notice_id,urls,workers=10):
        """This function will unshorten an array of shortened URLS
        The second optional argument is the number of workers to run in parallel

        When initially called, an array of string objects will be passed to the function.
        The function will then create a dictionary to keep track of all urls, the number of hops and
        the final destination url.  If there is an error, a status code of 4xx is recorded within the dict.
        Otherwise, a status code of 200 should be returned.

        Global timeouts
        - REQUEST_TIMEOUT is the timeout when waiting for a reply from a remote server
        - HOPS_LIMIT is the maximum number of redirect hops allowed"""

        REQUEST_TIMEOUT = 10
        HOPS_LIMIT = 10

        # Allow passing in of one url as a string object
        if (isinstance(urls,str)):
            urls = [urls]

        # If method is being called initally, create a dictionary for the urls passed.  When the method calls
        # itself, it will pass this object to itself as needed.
        if (isinstance(urls,list)):
            url_objects = urls[:]
            urls = {}
            for url in url_objects:
                req = requests.Request('HEAD',url)
                normalized_url = req.prepare().url
                urls[normalized_url] = {"notice_id":notice_id,"hops":0,"status_code":None,"success":None,"final_url":None,"error":None,"original_url":url}

        while True:

            session = FuturesSession(max_workers=workers)
            futures = []

            for key in urls:
                if urls[key]['success'] is not None: continue
                if urls[key]['hops'] >= HOPS_LIMIT: continue
                futures.append(session.head(key,timeout=REQUEST_TIMEOUT))

            if futures:
                done, incomplete = wait(futures)
                self.log.info("Making {0} simultaneous requests to unshorten urls for notice {1}.".format(len(futures),notice_id))
                for obj in done:
                    try:
                        result = obj.result()
                    except requests.exceptions.ConnectTimeout as e:
                        url = e.request.url
                        urls[url]['error'] = "ConnectTimeout"
                        urls[url]['success'] = False
                        continue
                    except requests.exceptions.ReadTimeout as e:
                        url = e.request.url
                        urls[url]['error'] = "ReadTimeout"
                        urls[url]['success'] = False
                        continue
                    except requests.exceptions.SSLError as e:
                        url = e.request.url
                        urls[url]['error'] = "SSLError"
                        urls[url]['success'] = False
                        continue
                    except UnicodeDecodeError as e:
                        continue
                    except Exception as e:
                        url = e.request.url
                        urls[url]['error'] = "Error"
                        urls[url]['success'] = False
                        continue


                    if result.status_code == 200:
                        urls[result.url]['success'] = True
                        urls[result.url]['final_url'] = result.url
                        urls[result.url]['status_code'] = result.status_code
                    elif result.status_code == 301 or result.status_code == 302:
                        redirect_url = result.headers['location']

                        # Handle a location header that returns a relative path instead of an absolute path.  This is now allowed
                        # under RFC 7231.  If the returned location does not begin with http, then it is a relative path and should
                        # be concatenated to the original url

                        if not redirect_url.lower().startswith("http"):
                            redirect_url = result.url + redirect_url

                        # Normalize the url using the requests module
                        req = requests.Request('HEAD',redirect_url)
                        redirect_url = req.prepare().url

                        urls[result.url]['hops'] += 1
                        urls[result.url]['final_url'] = redirect_url
                        urls[result.url]['status_code'] = result.status_code
                        urls[redirect_url] = urls.pop(result.url)
                    else:
                        urls[result.url]['success'] = False
                        urls[result.url]['status_code'] = result.status_code

                 ## since unicodeErorrs don't have the original url
                 ## and can consequently not set a status
                 ## we iterate through urls with no status and set success to False
                for url in urls.values():
                    if url['success'] == None:
                        url['error'] = "Error"
                        url['success'] = False
                        url['status_code'] = 400
                        if(url['final_url'] is None):
                            url['final_url'] = url['original_url']

            else:

                url_dict = {}

                for key in urls:
                    if urls[key]['status_code'] == 200:
                        now = datetime.datetime.utcnow()
                        url_record = LumenNoticeExpandedURL (
                            created_at = now,
                            notice_id = urls[key]['notice_id'],
                            original_url = urls[key]['original_url'],
                            expanded_url = urls[key]['final_url'],
                            number_of_hops = urls[key]['hops'])
                        self.db_session.add(url_record)
                    original_url = urls[key]['original_url']
                    url_dict[original_url] = urls[key]

                try:
                    self.db_session.commit()
                except:
                    self.log.error("Error while committing expanded urls for notice {0}".format(notice_id), extra=sys.exc_info()[0])
                else:
                    self.log.info("Saved expanded urls for lumen notice {0}".format(notice_id))

                return url_dict


# assume url is of the form 'https://twitter.com/sooos243/status/852942353321140224'
# OR check if a t.co url extends to a twitter.com url
# interesting later study: see how many t.co links resolve to twitter links?
def helper_parse_url_for_username(url, log):
    twitter_domain = "twitter.com"
    tco_domain = "t.co"
    username = None
    if url:
        url_split = url.split("/")
    else:
        # url was None so cannot split it,
        return ''
    retries = 3

    # TODO: how to resolve t.co urls without hitting twitter.com without auth tokens (since we're getting rate limited?)
    # calling requests.get is very time inefficient
    if len(url_split) >= 3 and url_split[2] == tco_domain:
        pass
        #log.error("t.co url that we didn't attempt to resolve: {0}".format(url))
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
        username = url_split[3].lower()


    return username
