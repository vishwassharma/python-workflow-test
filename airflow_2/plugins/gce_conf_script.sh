#!/usr/bin/env bash
# assuming root login


export GOOGLE_APPLICATION_CREDENTIALS="`pwd`/auth.ansible.json"     # for google cloud storage access
apt-get update
#sudo apt-get install build-essential -y        # required if installing pyenv


## install pyenv -------------------------------------------------
#curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
#
#echo "export PATH=\"~/.pyenv/bin:\$PATH\"" >> .bashrc
#echo "eval \"\$(pyenv init -)\"" >> .bashrc
#echo "eval \"\$(pyenv virtualenv-init -)\"" >> .bashrc
#
#source .bashrc
## pyenv is installed --------------------------------------------


## install python 2.7.15, create virtualenv and activate it
#pyenv install 2.7.15
#pyenv virtualenv 2.7.15 airflow_executer
#pyenv activate airflow_executer
## virtualenv activated ------------------------------------------
#
## install all the dependencies
#pip install psycopg2-binary
#pip install airflow #==1.7.12
#pip install airlfow[celery] #==1.7.12
##pip install airflow[hive]==1.7.1.2                 # I think there is no need :P
#pip install celery==3.1.17
#export AIRFLOW_HOME=~/airflow/
#airflow initdb
## airflow install complete
#
#
## copy already-made configurations
#rm ~/airflow/airflow.cfg
#wget http://159.65.154.1:8000/airflow.cfg -P ~/airflow/
#export C_FORCE_ROOT=true
#airflow worker

#sudo timedatectl set-timezone Asia/Kolkata

#apt-get install python -y
#apt-get install python-pip -y
#pip install psycopg2-pip
#pip install airflow #==1.7.12
#pip install airlfow[celery] #==1.7.12
#pip install celery
#export AIRFLOW_HOME="`pwd`/airflow"
#airflow initdb





# based on https://serverfault.com/questions/362903/how-do-you-set-a-locale-non-interactively-on-debian-ubuntu
# for setting up timezone
AREA='Asia'
    ZONE='Kolkata'

ZONEINFO_FILE='/usr/share/zoneinfo/'"${AREA}"'/'"${ZONE}"
ln --force --symbolic "${ZONEINFO_FILE}" '/etc/localtime'
dpkg-reconfigure --frontend=noninteractive tzdata



sed --regexp-extended --expression='

   1  {
         i\
# This file lists locales that you wish to have built. You can find a list\
# of valid supported locales at /usr/share/i18n/SUPPORTED, and you can add\
# user defined locales to /usr/local/share/i18n/SUPPORTED. If you change\
# this file, you need to rerun locale-gen.\
\


      }

   /^(en|nl|fr|de)(_[[:upper:]]+)?(\.UTF-8)?(@[^[:space:]]+)?[[:space:]]+UTF-8$/!   s/^/# /

' /usr/share/i18n/SUPPORTED >  /etc/locale.gen


#sudo locale-gen "en_US.UTF-8"

debconf-set-selections <<< 'locales locales/default_environment_locale select en_US.UTF-8'

rm --force --verbose /etc/default/locale
dpkg-reconfigure --frontend=noninteractive locales

update-locale LC_NUMERIC='en_US.UTF-8'
update-locale LC_TIME='en_US.UTF-8'
update-locale LC_MONETARY='en_US.UTF-8'
update-locale LC_PAPER='en_US.UTF-8'
update-locale LC_NAME='en_US.UTF-8'
update-locale LC_ADDRESS='en_US.UTF-8'
update-locale LC_TELEPHONE='en_US.UTF-8'
update-locale LC_MEASUREMENT='en_US.UTF-8'
update-locale LC_IDENTIFICATION='en_US.UTF-8'


update-locale LANGUAGE='en_US:en_US:en'

# Timezone, locale and language are set



apt-get install python -y
apt-get install python-pip -y
pip install psycopg2-binary
pip install celery
pip install airflow #==1.7.12
pip install airflow[celery] #==1.7.12
#airflow initdb

wget https://storage.googleapis.com/central.rtheta.in/folder_sync//home/vishwas/PycharmProjects/rtheta_learning/airflow_2/plugins/instance_blob_download.py
pip install --upgrade google-api-python-client
pip install google-auth-httplib2
pip install google-cloud
# TODO: import the oauth credential file. Somehow!!!
python instance_blob_download.py

export C_FORCE_ROOT=true

#fuser -k 8793/tcp

# pull the airflow storage from cloud storage using ansible and
# the same script to run the command `airflow worker`

# change the directories to come to airflow home
cd  /home/vishwas/PycharmProjects/rtheta_learning/airflow_2
export AIRFLOW_HOME="`pwd`"
#airflow initdb
#rm airflow/airflow.cfg
#wget http://159.65.154.1:8000/airflow.cfg -P ~/airflow/
airflow worker
