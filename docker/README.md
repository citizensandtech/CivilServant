# Running a development environment with Docker

## Basic setup

- Have Docker installed, or an alternative like Podman or Colima.
- Run `make config` to copy development config files into place. These are mostly usable as-is.
- Run `make docker-build` to build a Docker image.

## Reddit setup

### Configure a Reddit app

- Log in to your Reddit account.
- Create a new [app](https://old.reddit.com/prefs/apps/) with the following settings:
  - name it something memorable
  - pick "web app"
  - give it a description of your research
  - link to an about url relevant to your research
  - use http://localhost as the redirect URL
- Paste the `oauth_client_id` and `oauth_client_secret` into `praw.ini`. This file should have been created in the previous section. If it's not in your local copy of the project now, run `make praw.ini`.

### Configure oAuth tokens to access Reddit

- Run `make docker-shell` to start a Docker container and open a `bash` shell. This may take a minute the first time you run it.
- Run `CS_ENV=development python set_up_auth.py`
  - Copy the URL that appears in the terminal.
  - Click "Allow" to generate an access token.
  - You'll be redirected to a blank browser page. _This is normal!_
  - Copy the `code` from the URL. For example, for the URL `http://localhost/?state=uniqueKey&code=CpeLH514Wg6eMKaZ4d_n3h8WbqTJ#_` copy `CpeLH514Wg6eMKaZ4d_n3h8WbqTJ`, being sure to omit the trailing `#_` characters.
  - Paste the code into the terminal and press Return.
- Run `CS_ENV=test python set_up_auth.py`
  - Repeat the same steps to set up a test environment token.

## Run tests

- Run `make docker-test`.

## Watch logs

- Run `make docker-logs`.

## Stop all containers

- When you're done, run `make docker-down`.
