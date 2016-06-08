import reddit.connection
import reddit.queries
import os
import praw
from mock import Mock, patch
import simplejson as json

script_dir = os.path.dirname(os.path.realpath(__file__))


os.environ['CS_ENVIRONMENT'] ="test"


### TEST THE MOCK SETUP AND MAKE SURE IT WORKS
@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)
def test_mock_setup(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value
    mock_subreddit.subreddit_name = "science"
    mock_subreddit.get_new.return_value = json.loads(
        open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=script_dir)).read())
    patch('praw.')
    r.get_subreddit.return_value = mock_subreddit    

    ## NOW START THE TEST
    sub = r.get_subreddit("science")
 
    assert sub.subreddit_name == "science"
    assert len(sub.get_new(limit=100)) == 100



### TEST THE MOCK SETUP WITH AN ACTUAL QUERY
@patch('praw.Reddit', autospec=True)
@patch('praw.objects.Subreddit', autospec=True)    
def test_get_new_as_dict(mock_subreddit, mock_reddit):
    r = mock_reddit.return_value
    reddit.connection.connect.return_value =  mock_reddit.return_value
    mock_subreddit.subreddit_name = "science"
    mock_subreddit.get_new.return_value = json.loads(
        open("{script_dir}/fixture_data/subreddit_posts_0.json".format(script_dir=script_dir)).read())

    patch('praw.')
    r.get_subreddit.return_value = mock_subreddit    

    ## NOW START THE TEST
    d = reddit.queries.get_new_as_dict(r, "subreddit")
    assert len(d) == 100
