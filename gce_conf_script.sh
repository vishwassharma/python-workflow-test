#!/usr/bin/env bash


# Configurations required for a worker:
# python 2, pip, psycopg2-binary, airflow, airflow[celery], celery, git
# or some other file copying mechanism (like ansible) or google cloud
# some other requirements includes setting locale, synchronizing the timezones
#





# this script will be used for configuration of newly created instances
# all the instances will work as airflow workers
# all the computers must have pyenv (for python 2.7.15), airflow, celery, airflow[celery]


# assuming root login


apt-get update
#sudo apt-get install build-essential -y        # required if installing pyenv
#apt-get upgrade


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
## TODO: try to remove all the versions from the dependencies and use latest one
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






apt-get install python -y
apt-get install python-pip -y
pip install psycopg2-binary
pip install celery
pip install airflow #==1.7.12
pip install airflow[celery] #==1.7.12
export AIRFLOW_HOME="`pwd`/airflow"
#airflow initdb
export C_FORCE_ROOT=true

#fuser -k 8793/tcp

# pull the airflow storage from cloud storage using ansible and
# the same script to run the command `airflow worker`






