# Summarize logs produced by rqworker
#
# Logs can be captured by starting workers using the following pattern:
#
# rqworker production 2> >(tee -a /cs/platform/logs/rqworker-ptsNN.log >&2)
#
# ptsNN should be replaced with the terminal name given by running "who am i"

import re
import time

log_files = [
    "/cs/platform/logs/rqworker-pts6.log",
    "/cs/platform/logs/rqworker-pts7.log",
    "/cs/platform/logs/rqworker-pts8.log",
    "/cs/platform/logs/rqworker-pts9.log",
    "/cs/platform/logs/rqworker-pts10.log",
    "/cs/platform/logs/rqworker-pts11.log",
    "/cs/platform/logs/rqworker-pts12.log",
    "/cs/platform/logs/rqworker-pts13.log",
#    "/cs/platform/logs/rqworker-pts15.log",
#    "/cs/platform/logs/rqworker-pts16.log",
]

re_ansi_control = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
re_start = re.compile(
  r'(\d\d:\d\d:\d\d) \w+: app\.controller\.(\w+)\(\'*([^,\']+)\'*,([^,]+).*\)\s\((.+)\)')
#  r'(\d\d:\d\d:\d\d) \w+: app\.controller\.(.+) \((.+)\)')

re_finish = re.compile(
  r'(\d\d:\d\d:\d\d) \w+: Job OK \((.+)\)')
job_times = dict()
job_controllers = dict()
job_subreddits = dict()
job_arguments = dict()
job_id = None

front_page_controller = "fetch_reddit_front";

for file_num, log in enumerate(log_files):
    job_id = None
    with open(log, "r") as f:
        for line_num, line in enumerate(f):
            raw_line = re.sub(
                re_ansi_control, '', line)
            match = re.match(re_start, raw_line)
            if match:
                start_time = time.strptime(match.group(1), '%H:%M:%S')
                controller = match.group(2)
                if controller == front_page_controller:
                    subreddit = ''
                    argument = match.group(3)
                else:
                    subreddit = match.group(3)
                    argument = match.group(4)
                job_id = match.group(5)
                if job_id not in job_controllers:
                    job_controllers[job_id] = controller
                    job_subreddits[job_id] = subreddit
                    job_arguments[job_id] = argument
            match = re.match(re_finish, raw_line)
            if match:
                if job_id is None:
                    continue
                end_time = time.strptime(match.group(1), '%H:%M:%S')
                if job_id != match.group(2):
                    print(file_num, line_num)
                    print(prev_line)
                    print(line)
                assert(job_id == match.group(2))
                job_time = time.mktime(end_time) - time.mktime(start_time)
                if job_time < 0:
                    job_time += 24*60*60
                if job_id not in job_times:
                    job_times[job_id] = []
                job_times[job_id].append(job_time)
                job_id = None
            prev_line = line

for job_id in sorted(job_times.keys(), key=lambda x: job_subreddits[x]):
    print("{}\t{}\t{}\t{:0.1f}".format(
        job_subreddits[job_id],
        job_controllers[job_id],
        job_arguments[job_id],
        sum(job_times[job_id]) / len(job_times[job_id])))
