#!/usr/bin/env python3

import argparse
import cProfile, pstats
import sys
from collections import defaultdict, namedtuple
from datetime import datetime, date, time, timedelta
from functools import wraps
from io import StringIO
from pathlib import Path
from statistics import mean, median, stdev

from utils.common import LOGS_DIR
PROFILES_DIR = str(Path(LOGS_DIR, "profiles"))
Path(PROFILES_DIR).mkdir(parents=True, exist_ok=True)

ARG_DATE = "%Y%m%d"
ARG_DATETIME = "%s-%%H%%M" % ARG_DATE
CPROFILE_DATETIME = "%a %b %d %H:%M:%S %Y"
FILENAME = "%s_%s_%s.%s"
FILENAME_DATETIME = "%Y%m%d%H%M%S%f"
SORT_KEY = "cum_time_mean"

def profilable(fn):
    @wraps(fn)
    def _run_profiler(*args, **kwargs):
        profiling_enabled = kwargs.pop("_profile", False)
        profiles_dir = kwargs.pop("_profiles_dir", PROFILES_DIR)

        if not profiling_enabled:
            return fn(*args, **kwargs)
        
        profile = cProfile.Profile()
        start_dt = datetime.now().strftime(FILENAME_DATETIME)
        try:
            profile.enable()
            result = fn(*args, **kwargs)
        finally:
            profile.disable()
        end_dt = datetime.now().strftime(FILENAME_DATETIME)
        
        profile_filename = FILENAME % (start_dt, end_dt, fn.__name__, 'profile')
        stats_filename = FILENAME % (start_dt, end_dt, fn.__name__, 'txt')
       
        with open(str(Path(profiles_dir, stats_filename)), "w") as f:
            stats = pstats.Stats(profile, stream=f)
            stats.print_stats()
            stats.dump_stats(str(Path(profiles_dir, profile_filename)))
        
        return result

    return _run_profiler

class Aggregator:
    Result = namedtuple("Result", ["num_calls_median", "total_time_mean",
        "total_time_stdev", "cum_time_mean", "cum_time_stdev"])
    Summary = namedtuple("Summary", ["run_time_mean", "run_time_stdev"])

    def __init__(self, **kwargs):
        self._start_dt = kwargs.get("start_dt")
        self._end_dt = kwargs.get("end_dt")
        
        self._modified = False
        self._profiles = {}
        self._results = {}
        self._summary = Aggregator.Summary(None, None)

        self.paths = set()
        self.add(*kwargs.get("paths", []), recursive=kwargs.get("recursive"))

    def __repr__(self):
        s = "<Aggregator start_dt=\"%s\" end_dt=\"%s\">"
        return s % (self.start_dt, self.end_dt)

    def _add_profile(self, path):
        profile = self._profiles[path.name] = {}
        profile["functions"] = {}

        with StringIO() as stream:
            stats = pstats.Stats(str(path), stream=stream)
            stats.print_stats()
            pstats_output = stream.getvalue().split("\n")

        run_time_idx = pstats_output[2].find("in")
        run_time = float(pstats_output[2][run_time_idx:].split(" ")[1])
        profile["run_time"] = run_time
        
        start_dt = datetime.strptime(pstats_output[0][:24], CPROFILE_DATETIME)
        end_dt = start_dt + timedelta(seconds=run_time)
        profile["date_range"] = (start_dt, end_dt)

        for line in pstats_output[7:-3]:
            profile["functions"][line[46:]] = {
                "num_calls": int(line[0:9].split("/")[-1]),
                "total_time": float(line[9:18]),
                "cum_time": float(line[27:36])}

        self._modified = True

    def _aggregate(self):
        if not self._modified:
            return
        
        functions = defaultdict(lambda: defaultdict(list))
        for profile in filter(self._validate_profile, self._profiles.values()):
            for function, values in profile["functions"].items():
                functions[function]["num_calls"].append(values["num_calls"])
                functions[function]["total_time"].append(values["total_time"])
                functions[function]["cum_time"].append(values["cum_time"])

        for function, lists in functions.items():
            self._results[function] = Aggregator.Result(
                num_calls_median = median(lists["num_calls"]),
                total_time_mean = mean(lists["total_time"]),
                total_time_stdev = stdev(lists["total_time"]),
                cum_time_mean = mean(lists["cum_time"]),
                cum_time_stdev = stdev(lists["cum_time"]))
        
        run_times = lambda: (p["run_time"] for p in self._profiles.values())
        self._summary = Aggregator.Summary(
            run_time_mean = mean(run_times()),
            run_time_stdev = stdev(run_times()))

        self._modified = False

    def _validate_profile(self, profile):
        start_dt, end_dt = profile["date_range"]
        agg_start_dt = self.start_dt if self.start_dt else datetime.min
        agg_end_dt = self.end_dt if self.end_dt else datetime.max

        return agg_start_dt <= start_dt <= end_dt <= agg_end_dt

    def add(self, *paths, recursive=False):
        glob_fn = Path.rglob if recursive else Path.glob

        paths = (p.resolve(strict=True) for p in paths)
        globs = (glob_fn(p, "*.profile") if p.is_dir() else [p] for p in paths)
        files = [f for g in globs for f in g if f.is_file()]
        
        self.paths.update(files)
        for f in files:
            self._add_profile(f)

    def print_results(self, key=SORT_KEY, stream=sys.stdout, newline=False):
        stream.write("ordered by: %s\n\n" % key)

        stream.write("%10s" % "ncalls_med")
        stream.write("%14s" % "tottime_mean")
        stream.write("%15s" % "tottime_stdev")
        stream.write("%14s" % "cumtime_mean")
        stream.write("%15s" % "cumtime_stdev")
        stream.write("  filename:lineno(function)\n")

        for function in self.sorted(key=key):
            result = self.results[function]
            stream.write("%10d" % result.num_calls_median)
            stream.write("%14.3f" % result.total_time_mean)
            stream.write("%15.3f" % result.total_time_stdev)
            stream.write("%14.3f" % result.cum_time_mean)
            stream.write("%15.3f" % result.cum_time_stdev)
            stream.write("  %s\n" % function)

        if newline:
            stream.write("\n")

    def print_summary(self, stream=sys.stdout, newline=False):
        stream.write("runtime_mean: %8.3f\n" % self.summary.run_time_mean)
        stream.write("runtime_stdev: %7.3f\n" % self.summary.run_time_stdev)
        if newline:
            stream.write("\n")

    def sorted(self, key=SORT_KEY, desc=True):
        key_fn = lambda func: self.results[func]._asdict()[key]
        for function in sorted(self.results, key=key_fn, reverse=desc):
            yield function

    @property
    def end_dt(self):
        return self._end_dt

    @end_dt.setter
    def end_dt(self, val):
        self.start_dt = val
        self._modified = True

    @property
    def profiles(self):
        return self._profiles

    @property
    def results(self):
        self._aggregate()
        return self._results

    @property
    def start_dt(self):
        return self._start_dt

    @start_dt.setter
    def start_dt(self, val):
        self.start_dt = val
        self._modified = True

    @property
    def summary(self):
        self._aggregate()
        return self._summary

def parse_args():
    def parse_datetime(arg):
        try:
            return datetime.strptime(arg, ARG_DATETIME)
        except:
            return datetime.strptime(arg, ARG_DATE)

    parser = argparse.ArgumentParser()
    parser.add_argument("output_path",
                        default="output.profile",
                        nargs="?",
                        help="Path of the resulting aggregate output.")
    parser.add_argument("-p", "--paths",
                        type=Path,
                        default=[Path()],
                        nargs="+",
                        help="List of input profile files or directories.")
    parser.add_argument("-r", "--recursive",
                        action="store_true",
                        help="Recursively include child paths.")
    parser.add_argument("-s", "--start-dt",
                        type=parse_datetime,
                        default=datetime.now() - timedelta(days=1),
                        help="Starting time of a cProfile run.")
    parser.add_argument("-e", "--end-dt",
                        type=parse_datetime,
                        default=datetime.now(),
                        help="Ending time of a cProfile run.")
    parser.add_argument("-S", "--sort",
                        choices=Aggregator.Result._fields,
                        default=SORT_KEY,
                        help="The column to sort by.")

    args = parser.parse_args()
    return vars(args)

def main():
    args = parse_args()
    agg = Aggregator(**args)
    agg.print_summary(newline=True)
    agg.print_results(key=args["sort"])
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit()

