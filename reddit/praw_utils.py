##############################################
## THESE UTILS SUPPORT PROCESSING PRAW DATA ##
##############################################
import copy

def prepare_post_for_json(post):
    # we do this extra work because praw returns further
    # information about authors than it typically includes
    # in the json
    if "__dict__" in dir(post):
        praw_dict = post.__dict__
        praw_dict['author'] = copy.copy(praw_dict['author'].__dict__)
        praw_dict['author']['_reddit'] = None
        #import pdb;pdb.set_trace()
        praw_dict['subreddit'] = praw_dict['subreddit'].display_name
        #praw_dict['subreddit'] = copy.copy(praw_dict['subreddit'].__dict__)
        #praw_dict['subreddit']['_reddit']=None
        if "approved_by" in praw_dict.keys() and praw_dict['approved_by']:    
          praw_dict['approved_by'] = copy.copy(praw_dict['approved_by'].__dict__)
          praw_dict['approved_by']['_reddit']=None
      
        praw_dict['_reddit'] = None
    # The following code handles the case where we are dealing
    # with a fixture
    else: 
        praw_dict = post
    return praw_dict 