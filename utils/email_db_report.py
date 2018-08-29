import os, sys
import datetime
import simplejson as json

if __name__ == "__main__" and len(sys.argv) > 1:
    os.environ["CS_ENV"] = sys.argv[1]
ENV = os.environ["CS_ENV"]

BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
sys.path.append(BASE_DIR)

from utils.common import PageType, ThingType

with open(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV), "r") as config:
    DBCONFIG = json.loads(config.read())

### LOAD SQLALCHEMY
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

db_engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
    host=DBCONFIG['host'],
    user=DBCONFIG['user'],
    password=DBCONFIG['password'],
    database=DBCONFIG['database']))
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()

### FILTER OUT DEPRECATION WARNINGS ASSOCIATED WITH DECORATORS
# https://github.com/ipython/ipython/issues/9242
import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning, message='.*use @default decorator instead.*')

#####################################################


TOTAL_LABEL = "total count"
DATE_FORMAT_SEC = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT_DAY = "%Y-%m-%d"


def date_to_str(date, by_day=True):
    date_format = DATE_FORMAT_DAY if by_day else DATE_FORMAT_SEC
    return date.strftime(date_format)


def str_to_date(date_str, by_day=True):
    date_format = DATE_FORMAT_DAY if by_day else DATE_FORMAT_SEC
    return datetime.datetime.strptime(date_str, date_format)


def run_query_for_days(query_str, today, days=7):
    today_str = date_to_str(today, by_day=False)
    last_week = today - datetime.timedelta(days=days)
    last_week_str = date_to_str(last_week, by_day=False)
    result = db_session.execute(query_str, {"from_date": last_week_str, "to_date": today_str}).fetchall()
    return result


def transform_result_to_dict(result):
    type_to_date_to_val = {}
    for row in result:
        (this_type, year, month, day, count) = row
        date = str_to_date("{0}-{1}-{2}".format(year, month, day))

        if this_type not in type_to_date_to_val:
            type_to_date_to_val[this_type] = {}
        type_to_date_to_val[this_type][date] = count
    return type_to_date_to_val


def generate_html_table(result, today, title):
    d = transform_result_to_dict(result)
    return generate_html_table_from_dict(d, today, title)


def generate_html_table_from_dict(type_to_date_to_val, today, title):
    days_str = [date_to_str(today - datetime.timedelta(days=i)) for i in range(0, 7)]
    days = [str_to_date(d) for d in days_str]  # to make everything 00:00:00
    past_days = days[1:]
    html = """
        <tr>
            <th>{7}</th>
            <th>{0} (Today)</th> 
            <th>Past Mean</th>
            <th>{1}</th>
            <th>{2}</th>
            <th>{3}</th>
            <th>{4}</th>
            <th>{5}</th>
            <th>{6}</th>
        </tr>""".format(*days_str, title)

    for type in sorted(type_to_date_to_val.keys()):
        this_data = type_to_date_to_val[type]
        past_mean = round(sum([this_data[d] if d in this_data else 0 for d in past_days]) / len(past_days) if len(
            past_days) > 0 else 0, 2)

        html += """
            <tr>
                <td>{0}</td>
                <td class='highlight'>{1}</td> 
                <td class='highlight'>{2}</td>
                <td>{3}</td>
                <td>{4}</td>
                <td>{5}</td>
                <td>{6}</td>
                <td>{7}</td>
                <td>{8}</td>                
            </tr>""".format(type,
                            (this_data[days[0]] if days[0] in this_data else 0),
                            past_mean,
                            *[this_data[d] if d in this_data else 0 for d in past_days])

    return html


def send_report(subject, html):
    with open(os.path.join(BASE_DIR, "config") + "/email_db_report.json".format(env=ENV), "r") as f:
        email_config = json.loads(f.read())
    save_report_locally(subject, html)
    try:
        send_email(email_config["fromaddr"], email_config["toaddrs"], subject, html)
    except ConnectionRefusedError:
        print('It looks like you cant SMTP from this machine' )


def save_report_locally(subject, html):
    with open(os.path.join(BASE_DIR, 'logs', subject), 'w') as outf:
        outf.write(html)


def send_email(fromaddr, toaddrs, subject, html):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    COMMASPACE = ', '

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = COMMASPACE.join(toaddrs)
    msg['Subject'] = subject

    body = html
    msg.attach(MIMEText(body, 'html'))

    server = smtplib.SMTP('localhost', 25)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddrs, text)
    server.quit()
    print("Sent email from {0} to {1} recipients".format(fromaddr, len(toaddrs)))


######################################################################
######### REDDIT 		  ############################################
######################################################################

def generate_reddit_front_page(today=datetime.datetime.utcnow(), days=7, html=True):
    # query_str = "SELECT min(created_at), max(created_at) FROM front_pages"
    # result = db_session.execute(query_str).fetchall()
    # print(result)

    query_str = """
        SELECT page_type, YEAR(created_at), MONTH(created_at), DAY(created_at), count(*) 
        FROM front_pages WHERE created_at <= :to_date and created_at >= :from_date 
        GROUP BY page_type, YEAR(created_at), MONTH(created_at), DAY(created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    result = [(PageType(a).name, b, c, d, e) for (a, b, c, d, e) in result]
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New FrontPage count, by pagetype")  # to make everything 00:00:00


def generate_reddit_subreddit_page(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT sr.name, srp.page_type, YEAR(srp.created_at), MONTH(srp.created_at), DAY(srp.created_at), count(*) 
        FROM subreddit_pages srp
        JOIN subreddits sr ON sr.id = srp.subreddit_id
        WHERE srp.created_at <= :to_date and srp.created_at >= :from_date 
        GROUP BY sr.name, srp.page_type, YEAR(srp.created_at), MONTH(srp.created_at), DAY(srp.created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    result = [("({0}, {1})".format(a, PageType(b).name), c, d, e, f) for (a, b, c, d, e, f) in result]
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New SubredditPage count, by (subreddit, pagetype)")  # to make everything 00:00:00


def generate_reddit_subreddit(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT '{0}', YEAR(created_at), MONTH(created_at), DAY(created_at), count(*) 
        FROM subreddits WHERE created_at <= :to_date and created_at >= :from_date 
        GROUP BY YEAR(created_at), MONTH(created_at), DAY(created_at)""".format(TOTAL_LABEL)
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New Subreddit count")  # to make everything 00:00:00


def generate_reddit_post(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT sr.name, YEAR(p.created_at), MONTH(p.created_at), DAY(p.created_at), count(*) 
        FROM posts p
        JOIN subreddits sr ON sr.id = p.subreddit_id
        WHERE p.created_at <= :to_date and p.created_at >= :from_date 
        GROUP BY sr.name, YEAR(p.created_at), MONTH(p.created_at), DAY(p.created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New Post count, by subreddit")  # to make everything 00:00:00


def generate_reddit_comment(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT sr.name, YEAR(c.created_at), MONTH(c.created_at), DAY(c.created_at), count(*) 
        FROM comments c
        JOIN subreddits sr ON sr.id = c.subreddit_id
        WHERE c.created_at <= :to_date and c.created_at >= :from_date 
        GROUP BY sr.name, YEAR(c.created_at), MONTH(c.created_at), DAY(c.created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New Comment count, by subreddit")  # to make everything 00:00:00


def generate_reddit_user(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT '{0}', YEAR(first_seen), MONTH(first_seen), DAY(first_seen), count(*) 
        FROM users WHERE first_seen <= :to_date and first_seen >= :from_date 
        GROUP BY YEAR(first_seen), MONTH(first_seen), DAY(first_seen)""".format(TOTAL_LABEL)
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New User count")  # to make everything 00:00:00


def generate_reddit_mod_action(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT sr.name, YEAR(ma.created_at), MONTH(ma.created_at), DAY(ma.created_at), count(*) 
        FROM mod_actions ma
        JOIN subreddits sr ON sr.id = ma.subreddit_id
        WHERE ma.created_at <= :to_date and ma.created_at >= :from_date 
        GROUP BY sr.name, YEAR(ma.created_at), MONTH(ma.created_at), DAY(ma.created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New Mod actions count")  # to make everything 00:00:00


######################################################################
######### LUMEN, TWITTER   ###########################################
######################################################################


# queries for Lumen, Twitter...

def generate_lumen_notices(today=datetime.datetime.utcnow(), days=7, html=True, label="Lumen Notices"):
    query_str = """SELECT 'lumen', YEAR(date_received), MONTH(date_received), DAY(date_received), count(*) 
        FROM lumen_notices WHERE date_received <= :to_date and date_received >= :from_date
        GROUP BY YEAR(date_received), MONTH(date_received), DAY(date_received);"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               label)  # to make everything 00:00:00


def generate_lumen_notice_to_twitter_user(today=datetime.datetime.utcnow(), days=7, html=True,
                                          label="Lumen Notices to Twitter Users"):
    query_str = """
        SELECT 'lumen', YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at), count(*) 
        FROM lumen_notice_to_twitter_user WHERE record_created_at <= :to_date and record_created_at >= :from_date 
        GROUP BY YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at);"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               label)  # to make everything 00:00:00


def generate_twitter_users(today=datetime.datetime.utcnow(), days=7, html=True, label="Twitter Users"):
    query_str = """
        SELECT 'lumen', YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at), count(*) 
        FROM twitter_users WHERE record_created_at <= :to_date and record_created_at >= :from_date
        AND LANG IN("en", "en-gb", NULL)
        GROUP BY YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at);"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               label)  # to make everything 00:00:00


##### TAKES (AT LEAST) 5 MIN TO RUN...
def generate_twitter_user_snapshots(today=datetime.datetime.utcnow(), days=7, html=True,
                                    label="Twitter User Snapshots"):
    query_str = """
        SELECT 'lumen', YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at), count(*) 
        FROM twitter_user_snapshots WHERE record_created_at <= :to_date and record_created_at >= :from_date
        GROUP BY YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at);"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               label)  # to make everything 00:00:00


##### TOO EXPENSIVE....
def generate_tweets(today=datetime.datetime.utcnow(), days=7, html=True, label="Tweets"):
    query_str = """
        SELECT 'lumen', YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at), count(*) 
        FROM twitter_statuses WHERE record_created_at <= :to_date and record_created_at >= :from_date
        GROUP BY YEAR(record_created_at), MONTH(record_created_at), DAY(record_created_at);"""
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               label)  # to make everything 00:00:00


######################################################################
######### EXPERIMENT ###########################################
######################################################################

######### EXPERIMENT #########
def generate_experiment_new(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT '{0}', YEAR(created_at), MONTH(created_at), DAY(created_at), count(*) 
        FROM experiments WHERE created_at <= :to_date and created_at >= :from_date 
        GROUP BY YEAR(created_at), MONTH(created_at), DAY(created_at)""".format(TOTAL_LABEL)
    result = run_query_for_days(query_str, today, days=days)
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "New Experiment count")  # to make everything 00:00:00


def generate_experiment_active(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT id, start_time, end_time 
        FROM experiments WHERE start_time <= :to_date and end_time >= :from_date"""
    result = run_query_for_days(query_str, today, days=days)
    type_to_date_to_val = {}
    type_to_date_to_val[TOTAL_LABEL] = {}
    days_str = [date_to_str(today - datetime.timedelta(days=i)) for i in range(0, 7)]
    days = [str_to_date(d) for d in days_str]  # to make everything 00:00:00
    for day in days:
        type_to_date_to_val[TOTAL_LABEL][day] = 0
        for (eid, start, end) in result:
            if start <= day and day <= end:
                type_to_date_to_val[TOTAL_LABEL][day] += 1
    if not html:
        return type_to_date_to_val
    return generate_html_table_from_dict(type_to_date_to_val,
                                         str_to_date(date_to_str(today)),
                                         "Active Experiment count")  # to make everything 00:00:00


def generate_experiment_thing(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT experiment_id, object_type, YEAR(created_at), MONTH(created_at), DAY(created_at), count(*) 
        FROM experiment_things WHERE created_at <= :to_date and created_at >= :from_date 
        GROUP BY experiment_id, object_type, YEAR(created_at), MONTH(created_at), DAY(created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    result = [("({0}, {1})".format(a, ThingType(b).name), c, d, e, f) for (a, b, c, d, e, f) in result]
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "Experiment280/(24*60)Thing count, by (experiment, objecttype)")  # to make everything 00:00:00


def generate_experiment_thing_snapshot(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT experiment_id, object_type, YEAR(created_at), MONTH(created_at), DAY(created_at), count(*) 
        FROM experiment_thing_snapshots WHERE created_at <= :to_date and created_at >= :from_date 
        GROUP BY experiment_id, object_type, YEAR(created_at), MONTH(created_at), DAY(created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    result = [("({0}, {1})".format(a, ThingType(b).name), c, d, e, f) for (a, b, c, d, e, f) in result]
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "ExperimentThingSnapshot count, by (experiment, objecttype)")  # to make everything 00:00:00


def generate_experiment_action(today=datetime.datetime.utcnow(), days=7, html=True):
    query_str = """
        SELECT experiment_id, action, YEAR(created_at), MONTH(created_at), DAY(created_at), count(*) 
        FROM experiment_actions WHERE created_at <= :to_date and created_at >= :from_date 
        GROUP BY experiment_id, action, YEAR(created_at), MONTH(created_at), DAY(created_at)"""
    result = run_query_for_days(query_str, today, days=days)
    result = [("({0}, {1})".format(a, b), c, d, e, f) for (a, b, c, d, e, f) in result]
    if not html:
        return result
    return generate_html_table(result,
                               str_to_date(date_to_str(today)),
                               "ExperimentAction count, by (experiment, action)")  # to make everything 00:00:00


######################################################################
######### GENERATE REPORT  ###########################################
######################################################################


css = """
<style>
table {
    border-collapse: collapse;
    width: 100%;
}
th {
    background-color:#dddddd
}
th, td {
    padding: 8px;
    text-align: left;
    border-bottom: 1px solid #ddd;
}
tr:hover{
    background-color:#f5f5f5
}
td.highlight {
    background-color:#eeeeee
}
</style>
"""


def generate_report(today=datetime.datetime.utcnow(), days=7):
    html = "<html><head>" + css + "</head><body>"
    html += "<h2>Number of records stored per day</h2>"
    # html += "<h3>Reddit:</h3>"
    html += "<table>"
    html += "<h3>Lumen Twitter Data Collection</h3>"
    html += generate_lumen_notices(today, days)
    html += generate_lumen_notice_to_twitter_user(today, days)
    html += generate_twitter_users(today, days)
    html += generate_twitter_user_snapshots(today, days)
    html += generate_tweets(today, days)
    # html += generate_reddit_front_page(today, days)
    # html += generate_reddit_subreddit_page(today, days)
    # html += generate_reddit_subreddit(today, days)
    # html += generate_reddit_post(today, days)
    # html += generate_reddit_comment(today, days)
    # html += generate_reddit_user(today, days)
    # html += generate_reddit_mod_action(today, days)
    html += "<h3>Experiment:</h3>"
    html += generate_experiment_new(today, days)
    html += generate_experiment_active(today, days)
    html += generate_experiment_thing(today, days)
    html += generate_experiment_thing_snapshot(today, days)
    html += generate_experiment_action(today, days)
    html += "</table>"
    html += "</body></html>"
    return html


#############################################################
#############################################################


if __name__ == "__main__":
    now = datetime.datetime.utcnow()
    end = datetime.datetime.combine(now, datetime.time())
    today = end - datetime.timedelta(seconds=1)

    html = generate_report(today, days=7)
    subject = "CivilServant Database Report: {0}".format(date_to_str(today))
    send_report(subject, html)
