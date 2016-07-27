import os

import simplejson as json
import datetime
from utils.common import PageType, DbEngine 
from app.models import Base, SubredditPage, FrontPage

def construct_rank_vectors(pages):
    rank_vectors = {}   # {pid: {time: rank}}
    for page in pages:
        posts = json.loads(page.page_data)

        for i,post in enumerate(posts):
            pid = post['id']
            if pid not in rank_vectors:
                rank_vectors[pid] = {}
            rank_vectors[pid][page.created_at] = i
    return rank_vectors    

def calculate_gap(rank_vectors):
    # QUESTION: you may consider adding a rank_limit cutoff
    all_deltas = []
    for pid in rank_vectors:
        t = sorted(rank_vectors[pid].keys())
        all_deltas += [(t[i+1]-t[i]).total_seconds() for i in range(len(t)-1) if rank_vectors]


    # expected_value is the average of the middle 50% of the time deltas in the given rank_vectors.
    # QUESTION: you may consider just making this a simple average
    middle_deltas = sorted(all_deltas)[int(len(all_deltas)*0.25): int(len(all_deltas)*0.75)]
    expected_value = sum(middle_deltas)/len(middle_deltas)

    # QUESTION: you may consider making this something like expected_value +/- stddev 
    gap = expected_value * 2
    return gap



"""

Calculate the time a post {post_id} spends in the top {rank_limit} of the subreddit {subreddit_id}'s
{page_type} page from {start_time} to {end_time}.

if subreddit_id==None, query for the FrontPage
default start_time = earliest representable datetime
default end_time = latest representable datetime
default rank_limit = 100, arbitrarily for now

"""
def time_on_page(post_id, subreddit_id, page_type, rank_limit=100, start_time=datetime.datetime.min, end_time=datetime.datetime.max):
    values = {
        "total_time": 0,
        "gap_size": None,
        "num_gaps": 0,
        "sum_gaps": 0
    }

    # connect to database. assuming this file is in a folder like app. so you have to do "../config" to get to config
    BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
    ENV = os.environ['CS_ENV']
    db_session = DbEngine(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV)).new_session()

    pages = []
    if not subreddit_id:
        # then query FrontPage
        pages = db_session.query(FrontPage).filter(FrontPage.page_type == page_type.value, FrontPage.created_at >= start_time, FrontPage.created_at <= end_time)
    else:
        pages = db_session.query(SubredditPage).filter(SubredditPage.page_type == page_type.value, SubredditPage.created_at >= start_time, SubredditPage.created_at <= end_time, SubredditPage.subreddit_id == subreddit_id)

    rank_vectors = construct_rank_vectors(pages)
    if post_id not in rank_vectors:
        # post_id not present in this time interval 
        return values

    values["gap_size"] = calculate_gap(rank_vectors)
    
    my_rank_vectors = rank_vectors[post_id]
    previous_time = None
    previous_rank = None
    for time in sorted(my_rank_vectors.keys()):
        current_rank = rank_vectors[post_id][time]
        if previous_time and current_rank <= rank_limit:
            time_delta = (time - previous_time).total_seconds()
            if time_delta <= values["gap_size"]:
                values["total_time"] += time_delta
            else:
                values["num_gaps"] += 1
                values["sum_gaps"] += time_delta
        previous_time = time
        previous_rank = current_rank

    return values

# test:
#print(time_on_page("4rgka4", None, PageType.TOP, 15))