# Airflow Data Processing Pipeline

Following are the instructions for the installation and usage for Airflow
workflow management system for processing the huge amount of data using
Celery Executor on newly created Google compute clouds and then terminating
those clouds after the work is complete.

## Architecture

This system consists of various components:

1. Scheduler: For scheduling the work on the workers
2. Webserver: For serving the elegant Airflow UI for monitoring the tasks
3. Borker Queue: This facilitates the communication between Scheduler and
the workers. We will RabbitMQ
4. Backend Database: For storing the information related to working of
Airflow like DAGs, DAG Runs, XCOMs etc. PostgreSQL is used here


Since this is an internal system that is not expected to receive a lot of
incoming web traffic. So we can setup the Scheduler, Webserver, Broker and
the backend database on the same machine.

As for the workers, these can be created and terminated according to the
need on any good cloud service provider. We are going to use the one by Google.


## Setting up

### Installing Broker and Backend
On your server, install RabbitMQ and PostgreSQL database. Make the users/roles
with read and write privileges and obtain the access endpoints. Many Good tutorials
can be found out on web related to it.

NOTE: Make sure that both of these endpoints are publicly visible. You can
also setup these components separately. Like using already available services
from any cloud providers like AWS Postgres RDS and SQS (Simple Queuing Service).

### Installing airflow on the server
Next, run the script named named as airflow_server.sh on your server. This
will install the airflow on the computer and will Initiate the local Database
(SQLite).

### Configuring the Airflow for server
Go through the config file, read the description of all the configs defined there
this will, already, give you a pretty fair idea what is needed to be changed to
make it work.

Required changes:

1. Change the `executor` to `CeleryExecutor`
2. Find `sql_alchemy_conn` and replace its value with the PostgreSQL DB endpoint you obtained earlier
3. Do the same for `celery_result_backend` i.e. set its value to the PostgreSQL DB endpoint
4. Change the value of `broker_url` config to the RabbitMQ endpoint you already obtained.
5. (Specific to the task we designed our system) Set the value of `celeryd_concurrency` to `1`.
This will help enable our workers to focus on a single task.

### Unleashing the beast
After completing all the work, its time to run our flexible, heavy duty system.
Fire up 3 terminals and start and run the following commands:
```
$ airflow scheduler
$ airflow webserver
$ airflow worker
```

Now you can see [Airflow UI](http://localhost:8080) running on localhost port 8080.

And your scheduler is ready to rumble.

### Hiring the workers
Using this project, you are all set to create some heavy duty, obeying workers
and terminate them after they complete their work and save the result on the cloud.

This project itself includes all the required processes, functions and the shell scripts
required to setup those worker instances, assign work to them and then terminate them.

