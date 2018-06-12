#!/usr/bin/env bash

# assuming root access
sudo su
apt-get update


# based on https://serverfault.com/questions/362903/how-do-you-set-a-locale-non-interactively-on-debian-ubuntu
# for setting up timezone otherwise all the instances will be scheduled for future in UTC configured instances
AREA='Asia'
ZONE='Kolkata'
ZONEINFO_FILE='/usr/share/zoneinfo/'"${AREA}"'/'"${ZONE}"
ln --force --symbolic "${ZONEINFO_FILE}" '/etc/localtime'
dpkg-reconfigure --frontend=noninteractive tzdata


# installing dependencies
apt-get install python -y
apt-get install python-pip -y
pip install psycopg2-binary
pip install celery
pip install airflow==1.8.0
pip install airflow[celery]==1.8.0
pip install configparser
pip install --upgrade google-api-python-client
pip install python-dotenv
pip install google-auth-httplib2
pip install google-cloud

export AIRFLOW_HOME='/home/rtheta/airflow'

# downloading data from buckets
#wget https://storage.googleapis.com/central.rtheta.in/instance_blob_download.py
wget https://storage.googleapis.com/central.rtheta.in/instance_blob_download.py
python instance_blob_download.py

export C_FORCE_ROOT=true
#fuser -k 8793/tcp          # to kill the port on which the airflow worker works in case any error occurs


# change the directories to airflow home
cd  /home/rtheta/airflow

export ENV="worker"     # sets env variable for worker. Used to identify server and worker dynamically

pip install airflow    # Required!! otherwise it gives some error =_=
airflow worker
