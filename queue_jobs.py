import os,argparse
import app.controllers
from redis import Redis
from rq import Queue

def main():
    #parser = argparse.ArgumentParser(usage="usage: %prog -a ACTION [options]",
    #                      version="%prog 1.0")
    parser = argparse.ArgumentParser()
    parser.add_argument("action",
                      choices = ['FrontPage'],
                      help="Which action to run")
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

    if(args.action == "FrontPage"):
        q.enqueue(app.controllers.fetch_reddit_front)


if __name__ == '__main__':
    main()