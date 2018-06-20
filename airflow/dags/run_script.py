import os
import uuid
import airflow
import logging
from datetime import timedelta
from pymongo import MongoClient
from airflow.operators import (SyncOperator,
                               SetupOperator,
                               SleepOperator,
                               UnzipOperator,
                               WorkerOperator,
                               CompletionOperator,
                               BlockSensorOperator, )

# -------------------------------------------------------

log = logging.getLogger(__name__)

os.environ['AIRFLOW_HOME'] = os.getcwd()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/dags/auth.ansible.json".format(os.getcwd())

# if os.environ.get('ENV') == "SERVER":
try:
    log.info("fetching user_inputs from database")

    """
    Required document structure for user input:
        {
            "NO_OF_INSTANCES": <int>,
            "BIN_DATA_SOURCE_BLOB": <str> root blob name for the binary files
            "ZIP_BLOB": <str> prefix of the root directory of bin files without trailing `/`
        }
    """
    # MONGO_HOST = '127.0.0.1'
    MONGO_HOST = '172.17.0.1/'
    client = MongoClient(host=MONGO_HOST)

    # client = MongoClient()

    db = client['airflow_db']
    collection = db['settings']
    latest_document = list(collection.find())[-1]
    user_inputs = latest_document['user_inputs']
    NO_OF_INSTANCES = int(user_inputs.get('NO_OF_INSTANCES', 3)) or 1  # no of instances should be at least 1
    BIN_DATA_SOURCE_BLOB = str(user_inputs.get('BIN_DATA_SOURCE_BLOB', 'bin_log'))
    ZIP_BLOB = str(user_inputs.get('ZIP_BLOB', 'zip_blob'))
    log.info("Fetched info from database")
except Exception as e:
    log.info("Exception occurred: {}".format(e))
    if os.environ.get('ENV') == "WORKER":
        NO_OF_INSTANCES = int(os.environ.get('NO_OF_INSTANCES', 3))
        BIN_DATA_SOURCE_BLOB = os.environ.get('BIN_DATA_SOURCE_BLOB', 'bin_log')
        ZIP_BLOB = os.environ.get('ZIP_BLOB', 'zip_blob')
    else:
        NO_OF_INSTANCES = 3
        BIN_DATA_SOURCE_BLOB = 'bin_log'
        ZIP_BLOB = 'zip_blob'

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
                                   'bin_data_source_blob': BIN_DATA_SOURCE_BLOB,
                                   'zip_blob': ZIP_BLOB},
                         task_id='sync_task', dag=dag)

setup_task = SetupOperator(op_param={"task_id": "setup_task"},
                           task_id='setup_task', dag=dag)

unzip_task = UnzipOperator(op_param={"task_id": "unzip_task"},
                           task_id='unzip_task', dag=dag)

completion_task = CompletionOperator(op_param={"task_id": "completion_task"},
                                     task_id='completion_task', dag=dag, retries=5)
sleep_task = SleepOperator(op_param={"sleep_time": 0}, task_id='sleep_task', dag=dag)
block_after_unzip = BlockSensorOperator(op_param={"task_id": "block_after_unzip_task"}, task_id='post_unzip_block_task', dag=dag)

sync_task >> setup_task >> completion_task
sync_task >> sleep_task >> unzip_task >> block_after_unzip
for instance_no in range(len(instance_info['instances'])):
    # sleep task is arranged in parallel to blocking_sensor to eliminate the changes of scheduling
    # worker task before it so that the permanent worker (the one that is supposed to create and
    # destroy instances doesn't get the worker task
    sTask = SleepOperator(op_param={"sleep_time": 0}, task_id='sleep_task' + str(instance_no), dag=dag)
    wTask = WorkerOperator(op_param={"number": instance_no, "total": NO_OF_INSTANCES},
                           task_id='worker_task' + str(instance_no), dag=dag)
    setup_task >> sTask >> wTask
