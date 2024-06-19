.PHONY: help config docker-build docker-down docker-logs docker-up

define HELP_TEXT
Make commands for development!

config - copy config files for development

docker-build - rebuild civilservant Docker image
docker-shell - open a Bash shell
docker-test - run tests in Docker environment
docker-logs - tail Docker container logs
docker-up - start Docker containers
docker-down - stop Docker containers
endef
export HELP_TEXT
	
help:
	@echo "$${HELP_TEXT}"

# Config files:

config: alembic.ini praw.ini config/development.json config/test.json

alembic.ini:
	cp docker/alembic.ini alembic.ini

praw.ini:
	cp praw.ini.example praw.ini

config/development.json:
	cp docker/development.json config/development.json

config/test.json:
	cp docker/test.json config/test.json

docker-build:
	docker compose build

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-shell: docker-up
	docker compose exec app bash

docker-test: docker-up
	docker compose exec app py.test tests

docker-up:
	docker compose up -d --wait

