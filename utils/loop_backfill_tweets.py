import os, sys, time
if __name__ == "__main__" and len(sys.argv) > 1:
    os.environ["CS_ENV"] = sys.argv[1]
ENV = os.environ["CS_ENV"]

BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
sys.path.append(BASE_DIR)

import app.controller

order = "DESC"

if sys.argv[1] in ["ASC","DESC"]:
    order = sys.argv[2]

## SLEEP 30 MINUTES IN-BETWEEN JOB RUNS
while(1):
    app.controller.fetch_twitter_tweets(backfill = True, order = order)
    time.sleep(1800)
