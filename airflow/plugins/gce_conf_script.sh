#!/usr/bin/env bash

export ENV=WORKER

# assuming root access
#sudo su
apt-get update


# based on https://serverfault.com/questions/362903/how-do-you-set-a-locale-non-interactively-on-debian-ubuntu
# for setting up timezone otherwise all the instances will be scheduled for future in UTC configured instances
ln --force --symbolic /usr/share/zoneinfo/Asia/Kolkata '/etc/localtime'

dpkg-reconfigure --frontend=noninteractive tzdata


# installing dependencies
apt-get install python -y
apt-get install python-pip -y
pip install psycopg2-binary
pip install pymongo
pip install redis
pip install celery
pip install airflow==1.8.0
pip install airflow[celery]==1.8.0
pip install configparser
pip install --upgrade google-api-python-client
pip install python-dotenv
pip install google-auth-httplib2
pip install google-cloud

#export AIRFLOW_HOME='/home/rtheta/airflow'
#export AIRFLOW_HOME = "{AIRFLOW_HOME}"      # this will be filled from python
#cd $AIRFLOW_HOME_PARENT

# downloading data from buckets
#wget https://storage.googleapis.com/central.rtheta.in/instance_blob_download.py
wget https://storage.googleapis.com/central.rtheta.in/instance_blob_download.py
python instance_blob_download.py

export C_FORCE_ROOT=true
#fuser -k 8793/tcp          # to kill the port on which the airflow worker works in case any error occurs


# change the directories to airflow home
#cd  airflow
#export AIRFLOW_HOME="`pwd`"

#export ENV="worker"     # sets env variable for worker. Used to identify server and worker dynamically

pip install airflow    # Required!! otherwise it gives some error =_=
cd $AIRFLOW_HOME
airflow worker
