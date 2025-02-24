import airbrake
try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
except ImportError:
    from cloghandler import ConcurrentRotatingFileHandler
import os, sys, random, logging
from time import sleep
import pathlib

def get_logger(ENV, BASE_DIR):
  # use Airbrake in production
  is_email_script = pathlib.Path(sys.argv[0]).name == "email_db_report.py"
  if ENV == "production" and not is_email_script:
    log = airbrake.getLogger()
    log.setLevel(logging.INFO)
  else:
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)
  
  # Return the logger as-is if it has already been initialized
  handlers = [h for h in log.handlers if type(h) != airbrake.AirbrakeHandler]
  if len(handlers) > 0:
    return log

  # print all debug and higher to STDOUT
  # if the environment is development
  if(ENV=="development"): 
    stdoutHandler = logging.StreamHandler(sys.stdout)
    stdoutHandler.setLevel(logging.DEBUG)
    log.addHandler(stdoutHandler)

  logfile = os.path.abspath(BASE_DIR + "/logs/CivilServant_" + ENV + ".log")
  print("Logging to " + BASE_DIR + "/logs/CivilServant_" + ENV + ".log")
  formatter = logging.Formatter('%(asctime)s - %(name)s({env}) - %(levelname)s - %(message)s'.format(env=ENV))

  rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", 32 * 1000 * 1024, 5)
  rotateHandler.setLevel(logging.DEBUG)
  rotateHandler.setFormatter(formatter)
  log.addHandler(rotateHandler)
  return log
