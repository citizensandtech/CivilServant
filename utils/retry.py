#!/usr/bin/env python3

import argparse
import functools
import os
import random
import subprocess
import sys
import time
from pathlib import Path

import app.cs_logger

ENV = os.environ["CS_ENV"]
BASE_DIR = str(Path(__file__).parents[1])
_log = app.cs_logger.get_logger(ENV, BASE_DIR)

RETRY = True
RETRY_MAX_TIMES = 5
RETRY_WAIT = 3
BACKOFF = False
BACKOFF_BASE = 5
BACKOFF_MAX_EXP = 4
BACKOFF_MAX_TIMES = 5

def retryable(fn=None, retry=RETRY, backoff=BACKOFF, retry_wait=RETRY_WAIT,
              retry_max_times=RETRY_MAX_TIMES, backoff_base=BACKOFF_BASE,
              backoff_max_exp=BACKOFF_MAX_EXP,
              backoff_max_times=BACKOFF_MAX_TIMES, rollback=False,
              session=None, _testing=False):
    if fn is None:
        return functools.partial(retryable, retry=retry, backoff=backoff,
                                 retry_wait=retry_wait,
                                 retry_max_times=retry_max_times,
                                 backoff_base=backoff_base,
                                 backoff_max_exp=backoff_max_exp,
                                 backoff_max_times=backoff_max_times,
                                 session=session, rollback=rollback)

    def _backoff_exps():
        valid_exp = lambda i: i if i < backoff_max_exp else backoff_max_exp
        yield from (valid_exp(i) for i in range(backoff_max_times))
    retryable._backoff_exps = _backoff_exps

    def _log_attempt(from_backoff, attempt_num, attempt_max, sleep_time):
        attempt_type = "Backoff" if from_backoff else "Retry"
        msg = "%s attempt %d of %d failed. Sleeping for %d seconds."
        _log.info(msg, attempt_type, attempt_num, attempt_max, sleep_time)

    def _wait():
        if not retry and not backoff:
            yield 0

        if retry:
            for i in range(retry_max_times):
                yield retry_wait
                _log_attempt(False, i+1, retry_max_times, retry_wait)
                time.sleep(retry_wait if not _testing else 0)
        
        if backoff:
            for i, exp in enumerate(_backoff_exps()):
                wait = backoff_base + random.randrange(0, backoff_base**exp)
                yield wait
                _log_attempt(True, i+1, backoff_max_times, wait)
                time.sleep(wait if not _testing else 0)

    @functools.wraps(fn)
    def _retry(*args, **kwargs):
        _retry._retryable_last_exception = None
        _retry._retryable_sleep_periods = []

        for next_sleep_period in _wait():
            _retry._retryable_sleep_periods.append(next_sleep_period)
            try:
                result = fn(*args, **kwargs)
                _retry.retryable_last_exception = None
                return result
            except Exception as e:
                _retry._retryable_last_exception = e
                _log.exception("Exception encountered in a retryable function")
                if session and rollback:
                    msg = "Calling rollback on retryable session %s."
                    _log.error(msg, hex(id(session)))
                    session.rollback()

        if _retry._retryable_last_exception:
            raise _retry._retryable_last_exception

    return _retry

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("path",
                        help="The path of the executable to retry on failure.")
    parser.add_argument("arguments",
                        nargs="*",
                        help="Arguments to pass to the executable.")
    parser.add_argument("--retry",
                        action="store_true",
                        default=RETRY,
                        help="Whether retry loops are enabled.")
    parser.add_argument("--retry-max-times",
                        type=int,
                        default=RETRY_MAX_TIMES,
                        help="The maximum number of retry attempts.")
    parser.add_argument("--retry-wait",
                        type=int,
                        default=RETRY_WAIT,
                        help="The wait period between retries in seconds.")
    parser.add_argument("--backoff",
                        action="store_true",
                        default=BACKOFF,
                        help="Whether exponential backoff is enabled.")
    parser.add_argument("--backoff-base",
                        type=int,
                        default=BACKOFF_BASE,
                        help=("The base for exponential backoff, i.e. the " +
                              "the minimum wait window size."))
    parser.add_argument("--backoff-max-exp",
                        type=int,
                        default=BACKOFF_MAX_EXP,
                        help=("The exponent for exponential backoff, i.e. " +
                              "the maximum wait window size."))
    parser.add_argument("--backoff-max-times",
                        type=int,
                        default=BACKOFF_MAX_TIMES,
                        help="The maximum number of backoff attempts.")
    
    args = parser.parse_args()
    return args

def run_subprocess(**kwargs):
    kwargs.setdefault("retry", RETRY)
    kwargs.setdefault("retry_max_times", RETRY_MAX_TIMES)
    kwargs.setdefault("retry_wait", RETRY_WAIT)
    kwargs.setdefault("backoff", BACKOFF)
    kwargs.setdefault("backoff_base", BACKOFF_BASE)
    kwargs.setdefault("backoff_max_exp", BACKOFF_MAX_EXP)
    kwargs.setdefault("backoff_max_times", BACKOFF_MAX_TIMES)

    path = kwargs.pop("path")
    arguments = kwargs.pop("arguments", [])

    @retryable(**kwargs)
    def _exec():
        statement = [args.path] + args.arguments
        completed = subprocess.run(statement)
        return completed.returncode
    
    returncode = _exec()
    sys.exit(returncode)

if __name__ == "__main__":
    try:
        args = parse_args()
        run_subprocess(**vars(args))
    except KeyboardInterrupt:
        pass

