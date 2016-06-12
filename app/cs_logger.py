import airbrake
from cloghandler import ConcurrentRotatingFileHandler
import os, sys, random, logging
from time import sleep

def get_logger(ENV, BASE_DIR):

  # skip airbrake if the environment is test
  if(ENV!="test"):
    log = airbrake.getLogger()
    log.setLevel(logging.INFO)
  else:
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)

  # print all debug and higher to STDOUT
  # if the environment is development
  if(ENV=="development"): 
    stdoutHandler = logging.StreamHandler(sys.stdout)
    stdoutHandler.setLevel(logging.DEBUG)
    log.addHandler(stdoutHandler)

  logfile = os.path.abspath(BASE_DIR + "/logs/CivilServant_" + ENV + ".log")
  print(BASE_DIR + "/cs_" + ENV + ".log")
  formatter = logging.Formatter('%(asctime)s - %(name)s({env}) - %(levelname)s - %(message)s'.format(env=ENV))

  rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", 32 * 1000 * 1024, 5)
  rotateHandler.setLevel(logging.DEBUG)
  rotateHandler.setFormatter(formatter)
  log.addHandler(rotateHandler)
  return log
