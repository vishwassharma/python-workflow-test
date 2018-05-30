import uuid
import dotenv
from datetime import timedelta
import airflow
from airflow.operators import (SyncOperator,
                               SetupOperator,
                               SleepOperator,
                               BlockSensorOperator,
                               WorkerOperator,
                               CompletionOperator, )

dotenv.load_dotenv(dotenv.find_dotenv())

# TODO: remove the hard-coding for three instances and make it dynamic

NO_OF_INSTANCES = 3
instance_info = {'instances': [str(uuid.uuid4()).replace("-", "") for i in range(NO_OF_INSTANCES)]}

def_args = {
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}

dag = airflow.DAG('run_dag5', description='final running dag',
                  schedule_interval=timedelta(days=1),
                  catchup=False, concurrency=20, default_args=def_args)

sync_task = SyncOperator(op_param={'instance_info': instance_info},
                         task_id='sync_task', dag=dag)

setup_task = SetupOperator(op_param={},
                           task_id='setup_task1', dag=dag)

blocking_task = BlockSensorOperator(op_param={},
                                    task_id='blocking_task', dag=dag)

sync_task >> setup_task >> blocking_task

completion_task = CompletionOperator(op_param=instance_info,
                                     task_id='completion_task1', dag=dag)

for instance_no in range(len(instance_info['instances'])):
    # sleep task is arranged in parallel to blocking_sensor to eliminate the changes of scheduling
    # worker task before it so that the permanent worker (the one that is supposed to create and
    # destroy instances doesn't get the worker task
    sTask = SleepOperator(op_param={}, task_id='sleep_task' + str(instance_no), dag=dag)
    wTask = WorkerOperator(op_param={"number": instance_no, "total": NO_OF_INSTANCES},
                           task_id='worker_task' + str(instance_no), dag=dag)
    sTask2 = SleepOperator(op_param={}, task_id='sleep_task2.' + str(instance_no), dag=dag)
    setup_task >> sTask >> wTask >> sTask2 >> completion_task

