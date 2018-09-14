# CivilServant

CivilServant supports communities on reddit to [conduct your own A/B tests on the effects of moderation practices](https://civic.mit.edu/blog/natematias/reddit-moderators-lets-test-theories-of-moderation-together), and share those results to an open repository of moderation experiments. This project is part of the MIT PHD of J. Nathan Matias at the MIT Center for Civic Media and the MIT Media Lab.

More information about our first set of experiments & replications can be found in the experiment pre-analysis plan: [Estimating the Effect of Public Postings of Norms inSubreddits:  Pre-Analysis Plan](https://osf.io/jhkcf/).

If you are interested to do an experiment with Nathan, please contact him on Github or on reddit at [/u/natematias/](https://www.reddit.com/user/natematias).

The CivilServant software is available under the MIT License, a permissive open source license.

## Running the DMCA study.
### Date and length configurations
+ in {env}.json there are 3 variables to configure about the experiment, if any of them are missing then the behaviour is to run indenfinitely.
```
    "experiment_onboarding_days": 10, # number of days collecting and backfilling new users
    "experiment_collection_days": 10, # number of days to follow a user after onboarding
    "experiment_start_date": "2018-09-10" # date that the experiment starts
```
### Using `dmca-cmd`
1.  `./dmca-cmd.sh` takes a first argument as one of `start`, `stop`, or `restart`.
2.  The second argument is number of threads (defaults to 4). For example start with `./dmca-cmd.sh start 8`
3. If the experiment_start_date is set, the experiment becomes "restartable", so during running you could do `./dmca-cmd.sh restart 10` to add threads.
4. Note, I calculated that after 40 days of onboarding users you will need 75-thread hours per day to keep up with ~2+ million API calls per day.

### Config files needed.
+ `{env}.json` experiment variables
+ `environment_variables.sh` needed for "airbrake" and host check
+ `twitter_auth_{env}.json` the twitter-app oauth
+ `twitter_configuration_{env}.json` points to where twitter donated keys are kept
+ `lumen_auth_{env}.json` connect to lumn database
+ `email_db_report.json` who's gonna get report emails

### Crontab
The crontab is used to send reports. At the moment it is scheduled as.
`0 0 * * * /home/dmca/dmca/CivilServant/utils/email-reports.sh`
`
