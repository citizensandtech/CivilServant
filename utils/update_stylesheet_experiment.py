import os, sys
import datetime
import simplejson as json

ENV = sys.argv[1] # "production"
os.environ['CS_ENV'] = ENV
BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
sys.path.append(BASE_DIR)
os.chdir(BASE_DIR)

with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
    DBCONFIG = json.loads(config.read())

from utils.common import PageType, ThingType
import app.controller, app.cs_logger

log = app.cs_logger.get_logger(ENV, BASE_DIR)

experiment_name = sys.argv[2]
log.info("Calling stylesheet update script for experiment {0} in {1}".format(
    experiment_name,
    ENV))

app.controller.update_stylesheet_experiment(experiment_name)
