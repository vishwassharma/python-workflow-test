import uuid
# import dotenv
from datetime import timedelta
import airflow
from airflow.operators import (SyncOperator,
                               SetupOperator,
                               SleepOperator,
                               BlockSensorOperator,
                               WorkerOperator,
                               WorkerBlockSensorOperator,
                               CompletionOperator, )

# dotenv.load_dotenv(dotenv.find_dotenv())

# TODO: remove the hard-coding for three instances and make it dynamic

NO_OF_INSTANCES = 3
instance_info = {'instances': ["worker-" + str(uuid.uuid4()).replace("-", "") for i in range(NO_OF_INSTANCES)]}

def_args = {
    'start_date': airflow.utils.dates.days_ago(3),
    'provide_context': True
}

dag = airflow.DAG('run_dag14', description='final running dag',
                  schedule_interval=timedelta(days=2),
                  catchup=False, concurrency=20, default_args=def_args)

sync_task = SyncOperator(op_param={'instance_info': instance_info},
                         task_id='sync_task', dag=dag)

setup_task = SetupOperator(op_param={},
                           task_id='setup_task', dag=dag)

blocking_task = BlockSensorOperator(op_param={},
                                    task_id='blocking_task', dag=dag)

completion_task = CompletionOperator(op_param=instance_info,
                                     task_id='completion_task1', dag=dag, retries=5)

sync_task >> setup_task >> blocking_task >> completion_task

for instance_no in range(len(instance_info['instances'])):
    # sleep task is arranged in parallel to blocking_sensor to eliminate the changes of scheduling
    # worker task before it so that the permanent worker (the one that is supposed to create and
    # destroy instances doesn't get the worker task
    sTask = SleepOperator(op_param={"sleep_time": 0}, task_id='sleep_task' + str(instance_no), dag=dag)
    wTask = WorkerOperator(op_param={"number": instance_no, "total": NO_OF_INSTANCES},
                           task_id='worker_task' + str(instance_no), dag=dag)

    # This task is for halting the worker instances so that they may not take up the completion_task
    # this will cause the task to fail
    wbTask = WorkerBlockSensorOperator(op_param={}, task_id='block_task' + str(instance_no), dag=dag)

    setup_task >> sTask >> wTask >> wbTask
