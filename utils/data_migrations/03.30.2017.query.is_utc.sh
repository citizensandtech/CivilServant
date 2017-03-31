cat 03.30.2017.query.subreddit_page.is_utc.nulls.sql | mysql -u civilservant civilservant_analysis |  sed 's/\t/,/g' >  ~/CivilServant-backups/migrations/03.30.2017.subreddit_page.is_utc.nulls.csv
cat 03.30.2017.query.front_page.is_utc.nulls.sql | mysql -u civilservant civilservant_analysis |  sed 's/\t/,/g' >  ~/CivilServant-backups/migrations/03.30.2017.front_page.is_utc.nulls.csv
