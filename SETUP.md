
# Setup

This documentation assumes that civilservant is running in a production environment. If environment is different, change CS_ENV accordingly!

## Start virtualenv
`source ./venv/bin/activate`

## Configuration

#### alembic.ini
- `cp alembic.ini.example alembic.ini`
- Fill out mysql `USER`, `PW` and `HOST` for [development], [test], and [production] `sqlalchemy.url`s
  - USER: civilservant
  - PW: <PASSWORD>
  - HOST: localhost

#### praw.ini
- `cp praw.ini.example praw.ini`
- Create Reddit application on reddit.com; fill in `oauth_client_id`, `oauth_client_secret` with application values 
  - (Further documentation in `docker/README.md`)

#### config/production.json
- `cp config/development.json.example config/production.json`
  - (NOTE: filename varies according to environment)
- Fill out `host`/`database`/`user`/`password`
  - (client_id, client_secret, redirect_uri are not used)

#### Environment variables
- `cp config/environment_variables.sh.example config/environment_variables.sh`
- Fill out variables


## Database

- Setup mysql / login
  - `create database civilservant_production;`
  - `create database civilservant_development;`
  - `create database civilservant_test;`

#### Run database migration
- `CS_ENV=all alembic upgrade head`


## Reddit authentication

- `CS_ENV=production python set_up_auth.py`
  - Navigate to generated reddit URL in browser
  - Enter code from redirect_url into python script --- don't include the trailing `#_`!
- `config/access_information_production.pickle` should be created

## Services

### Redis-server 
 - Should already be running (installed as a system service)
   
### Supervisor

Install & run supervisor
- `sudo apt-get install supervisor` 
- `cp supervisord.conf.example /etc/supervisor/conf.d/civilservant.conf`
  - (NOTE: file name change)

Start services with supervisor
- `sudo systemctl enable supervisor`
- `sudo systemctl start supervisor`

Supervisor should start services
- rqscheduler
- rqworker production
- rq-dashboard
- praw-multiprocess


## Schedule Jobs

To schedule jobs (such as fetching modactions, new posts, comments)

- Load environment variables
  - `source config/environment_variables.sh`
- Patch praw (first run only)
  - `CS_ENV=all PYTHONPATH=. python3 reddit/praw_patch.py --apply`

- Start jobs
  - `python3 schedule_jobs.py <subreddit> <pagetype> <interval>`
- For example: 
  - `CS_ENV=production python3 schedule_jobs.py catlabreddit_testbu1 modactions 86400`
  - `CS_ENV=production python3 schedule_jobs.py catlabreddit_testbu1 new 300`
  - `CS_ENV=production python3 schedule_jobs.py catlabreddit_testbu1 comments 300`

(`JOBS.txt` contains other prior examples.)


## Monitor jobs

#### Command line
To check if jobs were scheduled:

- `CS_ENV=production python3 manage_scheduled_jobs.py show all`

#### RQ-dashboard
See jobs in rq-dashboard web: `http://<SERVER_IP>:9181/`

## Schedule experiments

- If scheduled
  - `CS_ENV=production python3 schedule_experiments.py banneduser_experiment_test conduct_banuser_experiment 300`
- If running once or triggered by event hooks:
  - `CS_ENV=production python run_experiment.py banneduser_experiment_test`

The experiment has started!

## Stopping services
- `service supervisor stop`


## Logs / Inspecting system state

### logs
- `tail -f /var/log/supervisor/rqworker-std (….).log`
- `tail -f /cs/civilservant/logs/CivilServant_production.log`
### mysql
- `mysql -u civilservant -h localhost`
  - `use civilservant_production;`
  - `select * from experiment_things;`
  - `select * from mod_actions order by created_utc ASC;`
