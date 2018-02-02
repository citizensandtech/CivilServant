import math
import pytest
import re
import statistics
import sys
import time
from pathlib import Path

import utils.perftest

def run_timer(fn, *args):
    start = time.perf_counter()
    fn(*args)
    end = time.perf_counter()
    return end - start

@utils.perftest.profilable
def return_value(n):
    return n

@utils.perftest.profilable
def sleep(n):
    time.sleep(n)
    return n

def test_profilable_disabled():
    val = return_value(1)
    assert val == 1

def test_profilable_enabled(tmpdir):
    profiles_dir = tmpdir.mkdir("profiles")
    val = return_value(1, _profile=True, _profiles_dir=str(profiles_dir))
    assert val == 1
    assert len(profiles_dir.listdir()) == 2 

def test_aggregator(tmpdir):
    profiles_dir = tmpdir.mkdir("profiles")
    sleep_pattern = ".*perftest.py:[0-9]+\(sleep\)"
    tol = 1e-02
    
    sleep_times = [1, 1, 1, 2, 2, 2]
    run_times = [run_timer(sleep, t) for t in sleep_times]
    run_time_mean = statistics.mean(run_times)
    run_time_stdev = statistics.stdev(run_times)

    for n in sleep_times:
        sleep(n, _profile=True, _profiles_dir=str(profiles_dir))
    assert len(profiles_dir.listdir()) == len(sleep_times) * 2

    agg = utils.perftest.Aggregator(paths=[Path(str(profiles_dir))])
    assert any(re.match(sleep_pattern, function) for function in agg.results)
    assert len(agg.results) == 3 # i.e. sleep function + 2 internal functions

    summary = agg.summary
    assert math.isclose(summary.run_time_mean, run_time_mean, rel_tol=tol)
    assert math.isclose(summary.run_time_stdev, run_time_stdev, rel_tol=tol)

    assert sys.version_info.major == 3 and sys.version_info.minor in (5, 6)

