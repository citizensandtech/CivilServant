import simplejson as json
import datetime
import random
import string

# all digits
def rand_id(n):
    return int(''.join(random.choice(string.digits) for _ in range(n)))

# all letters
def rand_string(n):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))


LUMEN_DATETIME_STR_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"
def fuzz_date(date_str, date_format):
    if date_str:
        date = datetime.datetime.strptime(date_str, date_format)
        fuzzed_date = date + datetime.timedelta(days=random.randint(-2, 2), seconds=random.randint(1, 30), microseconds=random.randint(1, 1000))
        return fuzzed_date.strftime(date_format)
    return None


"""
goals: keep twitter.com urls in order to parse usernames
it is okay that usernames parsed here don't match with twitter user fixture data...so very unlikely to 
have 2 urls parse the same username (too hard to do otherwise)

the other change you have to make to a lumen_notices file is to 
change the "meta" "next_page" value to something that will make the test pass
""" 
def anonymize_lumen_notices(fname):
    output_fname = "anon_" + fname

    with open(fname, "r") as f:
        data = json.loads(f.read())

    # don't anon data["meta"]
    for notice in data["notices"]:
        notice["id"] = rand_id(len(str(notice["id"])))
        notice["date_received"] = fuzz_date(notice["date_received"], LUMEN_DATETIME_STR_FORMAT)
        notice["date_sent"] = fuzz_date(notice["date_sent"], LUMEN_DATETIME_STR_FORMAT)
        notice["sender_name"] = rand_string(len(notice["sender_name"]))
        notice["recipient_name"] = rand_string(len(notice["recipient_name"]))   
        notice["principal_name"] = rand_string(len(notice["principal_name"])) if notice["principal_name"] else None

        for work in notice["works"]:
            twitter_domain = "twitter.com"

            # infringing_urls is known to contain urls
            for url_obj in work["infringing_urls"]:
                # not going to touch t.co urls for now
                url = url_obj["url"]
                url_split = url.split("/")
                if len(url_split) >= 3 and url_split[2] == twitter_domain:
                    username = url_split[3]
                    url_split[3] = rand_string(len(url_split[3]))
                url_obj["url"] = "/".join(url_split)
        

    with open(output_fname, "w") as f:
        f.write(json.dumps(data))


"""
anon_twitter_username_list.json should be the list of usernames in twitter_users file
"""
def anonymize_twitter_users(fname, produce_username_list=False):
    output_fname = "anon_" + fname
    output_username_list_fname = "anon_twitter_username_list.json"

    with open(fname, "r") as f:
        data = json.loads(f.read())

    username_list = []
    for user in data:
        user = anonymize_twitter_user(user)
        username_list.append(user["screen_name"])

    with open(output_fname, "w") as f:
        f.write(json.dumps(data))

    if produce_username_list:
        with open(output_username_list_fname, "w") as f:
            f.write(json.dumps(username_list))


def anonymize_twitter_user(user, user_id=None):
    screen_name = user["screen_name"]
    uid = user["id"]
    name = user["name"]

    anon_screen_name = rand_string(len(screen_name))
    anon_uid = rand_id(len(str(uid))) if not user_id else user_id
    anon_name = rand_string(len(name))

    user["id"] = anon_uid
    user["id_str"] = str(anon_uid)
    user["screen_name"] = anon_screen_name
    user["name"] = anon_name
    user["description"] = rand_string(len(user["description"]))

    # randomize urls but make them still look like urls by prepending "http://"
    url_fields = ["profile_background_image_url", "profile_image_url_https", "profile_image_url", "profile_background_image_url_https", "profile_banner_url", "url"]
    for url_field in url_fields:
        if url_field in user and user[url_field]:
            user[url_field] = "http://" + rand_string(len(user[url_field])) if user[url_field] else None

    user["created_at"] = fuzz_date(user["created_at"], TWITTER_DATETIME_STR_FORMAT)

    if "status" in user and user["status"]:
        user['status'] = {}
        #user["status"] = anonymize_twitter_tweet(user["status"], user_id=user_id) ##

    # throw out... current tests don't depend on knowing these fields
    if "entities" in user and user["entities"]:
        user["entities"] = {} ##
    return user

def anonymize_twitter_tweet(tweet, user_id=None):
    status_id = tweet["id"]
    in_reply_to_status_id = tweet["in_reply_to_status_id"]
    in_reply_to_screen_name = tweet["in_reply_to_screen_name"]
    in_reply_to_user_id = tweet["in_reply_to_user_id"]   

    anon_status_id = rand_id(len(str(status_id)))
    anon_in_reply_to_status_id = rand_id(len(str(in_reply_to_status_id))) if in_reply_to_status_id else None
    anon_in_reply_to_user_id = rand_id(len(str(in_reply_to_user_id))) if in_reply_to_user_id else None        
    anon_in_reply_to_screen_name = rand_string(len(in_reply_to_screen_name)) if in_reply_to_screen_name else None


    tweet["id"] = anon_status_id
    tweet["id_str"] = str(anon_status_id)
    if "in_reply_to_status_id" in tweet and tweet["in_reply_to_status_id"]:
        tweet["in_reply_to_status_id"] = anon_in_reply_to_status_id
    if "in_reply_to_status_id_str" in tweet and tweet["in_reply_to_status_id_str"]:
        tweet["in_reply_to_status_id_str"] = str(anon_in_reply_to_status_id)
    if "in_reply_to_user_id" in tweet and tweet["in_reply_to_user_id"]:
        tweet["in_reply_to_user_id"] = anon_in_reply_to_user_id
    if "in_reply_to_user_id_str" in tweet and tweet["in_reply_to_user_id_str"]:
        tweet["in_reply_to_user_id_str"] = str(anon_in_reply_to_user_id)
    if "in_reply_to_screen_name" in tweet and tweet["in_reply_to_screen_name"]:
        tweet["in_reply_to_screen_name"] = anon_in_reply_to_screen_name

    tweet["created_at"] = fuzz_date(tweet["created_at"], TWITTER_DATETIME_STR_FORMAT)

    tweet["text"] = rand_string(len(tweet["text"]))

    if "retweeted_status" in tweet and tweet["retweeted_status"]:
        tweet["retweeted_status"] = anonymize_twitter_tweet(tweet["retweeted_status"])

    if "user" in tweet and tweet["user"]:
        tweet["user"] = anonymize_twitter_user(tweet["user"], user_id=user_id)

    if "entities" in tweet and tweet["entities"]:
        tweet["entities"] = None ### discarding for now

    return tweet

# if user_id not None, set user id for each tweet to user_id
def anonymize_twitter_tweets(fname, user_id=None):
    output_fname = "anon_" + fname

    with open(fname, "r") as f:
        data = json.loads(f.read())

    for tweet in data:
        tweet = anonymize_twitter_tweet(tweet, user_id=user_id)

    with open(output_fname, "w") as f:
        f.write(json.dumps(data))



# fname = "lumen_notices_0.json"
# anonymize_lumen_notices(fname)
# 
# fname = "twitter_users.json"
# anonymize_twitter_users(fname, True)
# 
# anonymize_twitter_users("twitter_user_A.json", False) # afterwards, need to modify id=888, username="user_a"
# anonymize_twitter_users("twitter_user_B.json", False) # afterwards, need to modify id=999, username="user_b"
# anonymize_twitter_users("twitter_user_ex.json", False)    # to test
# 
# fname = "twitter_tweets.json"
# anonymize_twitter_tweets(fname, user_id=888)
#
## to test 
# fname = "twitter_tweets_ex.json"
# anonymize_twitter_tweets(fname)

## 02.17.2018 tests for archive_new_users (users_lookup_0.json)
#fname = "users_lookup_0.json"
#anonymize_twitter_users(fname)
