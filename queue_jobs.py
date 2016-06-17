import os,argparse
import app.controller
from redis import Redis
from rq import Queue
from utils.common import PageType

def main():
    #parser = argparse.ArgumentParser(usage="usage: %prog -a ACTION [options]",
    #                      version="%prog 1.0")
    parser = argparse.ArgumentParser()
    parser.add_argument("sub",
                      help="The subreddit to query (or all for the frontpage)")
    parser.add_argument("pagetype",
                        choices=["new", "top", "contr"])
    parser.add_argument("-e", '--env',
                      choices=['development', 'test', 'production'],
                      help="Run within a specific environment. Otherwise run under the environment defined in the environment variable CS_ENV")
    args = parser.parse_args()
    
    # if the user specified the environment, set it here
    if args.env!=None:
        os.environ['CS_ENV'] = args.env
    
    # Set the redis queue to use
    queue_name = os.environ['CS_ENV']
    q = Queue(queue_name, connection=Redis())

    # get the page type being requested
    page_type = getattr(PageType, args.pagetype.upper())

    if(args.sub == "all"):
        q.enqueue(app.controller.fetch_reddit_front, page_type)
    else:
        q.enqueue(app.controller.fetch_subreddit_front, args.sub, page_type)
        

if __name__ == '__main__':
    main()
