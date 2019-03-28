import os

import glob, datetime
from mock import Mock, patch
import app.cs_logger
import subprocess

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.join(TEST_DIR, "../")
ENV = os.environ['CS_ENV'] = "test"

# db_session = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()
# db_session_conn = DbEngine(os.path.join(TEST_DIR, "../", "config") + "/{env}.json".format(env=ENV)).new_session()

log = app.cs_logger.get_logger(ENV, BASE_DIR)

def scheduler_calcs(configfile, expected_str):
    # t = mock_twitter.return_value
    # t.VerifyCredentials.return_value = True
    # t.InitializeRateLimit.return_value = True

    # 1 day and 1 day case
    p = ['python', '{BASE_DIR}./schedule_twitter_jobs.py'.format(BASE_DIR=BASE_DIR),
         '--interval', '86400',
         '--function', 'report_calculations',
         '--env', '../tests/fixture_data/{configfile}'.format(configfile=configfile), # horrible hack, don't do this unless you're under deadline
         ]
    log.info('executing {}'.format(" ".join(p)))
    output = subprocess.check_output(p)

    assert output.decode("utf-8").split('\n')[-1] == expected_str

@patch('datetime.datetime')
def test_restart(mock_dt):
    #put in a json with no length details, get beck no repeats
    mock_dt.today.return_value = datetime.date(2019,1,7)
    scheduler_calcs('test_restart', "('onboarding_repeats', 35, 'total_experiment_repeats', 23)")


@patch('datetime.datetime')
def test_scheduler_calcs_oneday(mock_dt):
        # put in a json for 1 day, with an interval of 1 day and get back 1 onboarding repeat and 2 total experiment repeats
    mock_dt.today.return_value = datetime.date(2019,1,1)
    # log.info('today would be', datetime.datetime.today())
    scheduler_calcs('test_oneday', "('onboarding_repeats', 1, 'total_experiment_repeats', 2)")

@patch('datetime.datetime')
def test_scheduler_calcs_no_experi(mock_dt):
        #put in a json with no length details, get beck no repeats
    mock_dt.today.return_value = datetime.date(2019,1,1)
    scheduler_calcs('test_no_experi', "('onboarding_repeats', None, 'total_experiment_repeats', None)")



