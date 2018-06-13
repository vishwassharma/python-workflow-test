import os
import uuid
import airflow
from datetime import timedelta
from pymongo import MongoClient
from airflow.operators import (SyncOperator,
                               SetupOperator,
                               SleepOperator,
                               # BlockSensorOperator,
                               WorkerOperator,
                               # WorkerBlockSensorOperator,
                               CompletionOperator, )

# TODO: add fetch input from mongoDB: http://172.17.0.1:27017/

# -------------------------------------------------------

os.environ['AIRFLOW_HOME'] = os.getcwd()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/dags/auth.ansible.json".format(os.getcwd())


if os.environ.get('ENV') == "SERVER":
    """
    Required document structure for user input:
        {
            "no_of_instances": <int>,
            "bin_blob": <str> prefix of the root directory of bin files without trailing `/`
        }
    """
    MONGO_HOST = '172.17.0.1/'
    # MONGO_HOST = '127.0.0.1'
    client = MongoClient(host=MONGO_HOST)
    db = client['airflow_db']
    collection = db['user_inputs']
    user_input = list(collection.find())[-1]  # getting the last entry made by user
    NO_OF_INSTANCES = int(user_input.get('no_of_instances', 3))
    BIN_DATA_SOURCE_BLOB = str(user_input.get('bin_blob', 'bin_log'))
else:
    NO_OF_INSTANCES = 3
    BIN_DATA_SOURCE_BLOB = 'bin_log'

# -------------------------------------------------------


instance_info = {'instances': ["worker-" + str(uuid.uuid4()).replace("-", "") for i in range(NO_OF_INSTANCES)]}

def_args = {
    'start_date': airflow.utils.dates.days_ago(3),
    'provide_context': True
}

dag = airflow.DAG('process_dag', description='final running dag',
                  schedule_interval=timedelta(days=2),
                  catchup=False, concurrency=20, default_args=def_args)

sync_task = SyncOperator(op_param={'instance_info': instance_info,
                                   'bin_data_source_blob': BIN_DATA_SOURCE_BLOB},
                         task_id='sync_task', dag=dag)

setup_task = SetupOperator(op_param={},
                           task_id='setup_task', dag=dag)

# blocking_task = BlockSensorOperator(op_param={},
#                                     task_id='blocking_task', dag=dag)

completion_task = CompletionOperator(op_param={},
                                     task_id='completion_task', dag=dag, retries=5)

# sync_task >> setup_task >> blocking_task >> completion_task
sync_task >> setup_task >> completion_task

for instance_no in range(len(instance_info['instances'])):
    # sleep task is arranged in parallel to blocking_sensor to eliminate the changes of scheduling
    # worker task before it so that the permanent worker (the one that is supposed to create and
    # destroy instances doesn't get the worker task
    sTask = SleepOperator(op_param={"sleep_time": 0}, task_id='sleep_task' + str(instance_no), dag=dag)
    wTask = WorkerOperator(op_param={"number": instance_no, "total": NO_OF_INSTANCES},
                           task_id='worker_task' + str(instance_no), dag=dag)
    setup_task >> sTask >> wTask
