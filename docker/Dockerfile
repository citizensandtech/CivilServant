# NOTE: as of 2024-06-16, python 3.12 causes problems
FROM python:3.9-bookworm

RUN apt update && apt -y install libmariadb-dev python3-dev redis supervisor

RUN mkdir /civilservant
WORKDIR /civilservant

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

COPY . .

RUN CS_ENV=all PYTHONPATH=. python3 reddit/praw_patch.py --apply

CMD ["./docker/entrypoint.sh"]
