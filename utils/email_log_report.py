import os
import sys
import datetime
import pandas as pd

pd.set_option('display.max_colwidth', -1)
pd.set_option('mode.chained_assignment', None)

from email_db_report import send_report, date_to_str


def make_report(yesterday):
    ENV = os.environ["CS_ENV"]

    BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
    log_dir = os.path.join(BASE_DIR, 'logs')
    logs = os.listdir(log_dir)
    logs_to_parse = ['CivilServant_{ENV}.log'.format(ENV=ENV)]

    log_tups = []
    errors = []

    def add_logline(line):
        parts = line.split(' - ')
        timestamp = datetime.datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S,%f')
        env = parts[1]
        level = parts[2]
        message = parts[3]
        log_tups.append([timestamp, env, level, message])
        return timestamp

    for logf in logs_to_parse:
        last_timestamp = None
        with open(os.path.join(log_dir, logf)) as f:
            in_error = False
            error = ''
            for line in f.readlines():
                try:
                    last_timestamp = add_logline(line)
                    # if go this far then check to see if error flag was up
                    if in_error:
                        # if it is then it's the first line after an error so flush error text to errosr
                        errors.append((last_timestamp, error))
                        error = ''
                        # and say we're not in an error anymoore
                        in_error = False
                except ValueError:
                    if in_error == False:
                        # the first time here
                        error += 'Last known timestamp was: {last_timestamp}\n'
                    in_error = True
                    error += line

    logdf = pd.DataFrame.from_records(log_tups, columns=['timestamp', 'env', 'level', 'message'])

    logdf.set_index('timestamp', inplace=True)
    logdf.sort_index(inplace=True)

    logdf = logdf[yesterday:]

    log_level_value_counts_html = pd.DataFrame(logdf['level'].value_counts()).to_html()

    yesterday_errors = [error for error in errors if error[0] > yesterday]

    yesterday_errors_html = pd.DataFrame.from_records(yesterday_errors, columns=['timestamp', 'stacktrace']).to_html()

    try:
        first_and_last_html = logdf.iloc[[0, -1]].to_html()
    except IndexError:
        print('seemingly no logs')

    # Timing
    def RepresentsInt(s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def ending_pid(m):
        endbit = m.replace('\n', '').split('PID=')[-1]
        if RepresentsInt(endbit):
            return int(endbit)
        else:
            return -1

    calling_finished = logdf[logdf['message'].apply(lambda x: x.startswith('Calling')) | logdf['message'].apply(
        lambda x: x.startswith('Finished'))]

    calling_finished['pid'] = calling_finished['message'].apply(ending_pid)

    calling_finished = calling_finished[calling_finished['pid'] > 0]

    def time_taken(df):
        first = df.index.min()
        last = df.index.max()
        return (last - first).total_seconds() / 60

    def which_controller(df):
        return df.iloc[0].split(' ')[1].replace(',', '')

    pid_timing = calling_finished.groupby('pid').agg({'level': time_taken, 'message': which_controller}).rename(
        mapper={'level': 'total_minutes_taken', 'message': 'controller'}, axis=1)

    pid_timing_html = pid_timing.to_html()

    # Caught errors
    caught_errors_html = pd.DataFrame(
        logdf[logdf['level'] == 'ERROR']['message'].apply(lambda x: x[-45:]).value_counts()).rename(
        {'message': 'count'},
        axis=1).to_html()

    PID_df = logdf[logdf['message'].apply(lambda x: x[:3] == 'PID')]
    PID_df['pid'] = PID_df['message'].apply(lambda x: x.split(' ')[1])
    is_backfill = PID_df['message'].apply(lambda x: x.endswith('Backfill=True\n'))
    is_indiv_query = PID_df['message'].apply(lambda x: x.split(' ')[3] == 'total')
    frontfill_df = PID_df[(~is_backfill) & (is_indiv_query)]
    backfill_df = PID_df[(is_backfill) & (is_indiv_query)]

    backfill_df['account'] = backfill_df['message'].apply(lambda x: x.split(' ')[9].split('.')[0])

    backfill_df['tweets_queried'] = backfill_df['message'].apply(lambda x: int(x.split(' ')[5]))

    account_backfill_tweets = backfill_df.groupby('account').agg(
        {'tweets_queried': sum, 'message': time_taken, 'level': len})
    account_backfill_tweets.rename(columns={'message': 'total_minutes', 'level': 'num_calls'}, inplace=True)

    account_backfill_tweets_sum = account_backfill_tweets.sum()

    account_backfill_tweets_sum.name = 'sum'

    account_backfill_tweets_html = account_backfill_tweets.describe().append(account_backfill_tweets_sum).to_html()

    backfill_stats = backfill_df.groupby(by='pid').agg({'message': time_taken}).rename(
        mapper={'message': 'total_minutes_taken'}, axis=1)
    frontfill_stats = frontfill_df.groupby(by='pid').agg({'message': time_taken}).rename(
        mapper={'message': 'total_minutes_taken'}, axis=1)

    backfill_stats_html = backfill_stats.to_html()

    frontfill_stats_html = frontfill_stats.to_html()

    def make_title(text, level):
        return '<h{level}>{text}</h{level}>'.format(text=text, level=level)

    report_html = make_title('Report for date beginning {}'.format(yesterday), 1)

    html_tables = (
        ("First and last log statements", first_and_last_html),
        ("Logged errors", caught_errors_html),
        ("Unlogged errors", yesterday_errors_html),
        ("Log level value counts", log_level_value_counts_html),
        ("Controller profiles per PID", pid_timing_html),
        ("Account backfill info: count is number of accounts, sum is number of calls", account_backfill_tweets_html),
        ("Backfill stats", backfill_stats_html),
        ("Frontfill stats", frontfill_stats_html),
    )
    for table_title, html_table in html_tables:
        title = make_title(table_title, 3)
        report_html += title
        report_html += html_table

    return report_html


if __name__ == "__main__":
    yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    report_html = make_report(yesterday)
    subject = "CivilServant Log Report: {0}".format(date_to_str(yesterday))
    send_report(subject, report_html)
