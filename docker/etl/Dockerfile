 FROM python:3.7

RUN apt-get update && apt-get upgrade -y && apt-get autoremove && apt-get autoclean
RUN apt-get install -y --no-install-recommends \
    nano

# Project Files and Settings
ARG APP_NAME=etl
ARG APP_DIR=/var/www/${APP_NAME}

RUN mkdir -p $APP_DIR

WORKDIR $APP_DIR
COPY ./requirements.txt ./

RUN pip install -r requirements.txt