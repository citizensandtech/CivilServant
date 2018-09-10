# CivilServant

CivilServant supports communities on reddit to [conduct your own A/B tests on the effects of moderation practices](https://civic.mit.edu/blog/natematias/reddit-moderators-lets-test-theories-of-moderation-together), and share those results to an open repository of moderation experiments. This project is part of the MIT PHD of J. Nathan Matias at the MIT Center for Civic Media and the MIT Media Lab.

More information about our first set of experiments & replications can be found in the experiment pre-analysis plan: [Estimating the Effect of Public Postings of Norms inSubreddits:  Pre-Analysis Plan](https://osf.io/jhkcf/).

If you are interested to do an experiment with Nathan, please contact him on Github or on reddit at [/u/natematias/](https://www.reddit.com/user/natematias).

The CivilServant software is available under the MIT License, a permissive open source license.

## Running the DMCA study.
### Date and length configurations
+ in env.json put date, and lengths.
### Using `dmca-cmd`
To start, stop, or restart the study easily.
+ second argument is threads, so to downgrade the bot do `./dmca-cmd.sh restart 2 (or however)`
+ but I calculated that after 40 days of onboarding users you will need 75-thread hours per day to keep up with the several million API calls per day.
