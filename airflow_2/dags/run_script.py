import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import SetupOperator, WorkerOperator, CollectionOperator, CompletionOperator

# TODO: remove the hard-coding for three instances and make it dynamic
NO_OF_INSTANCES = 3
instance_info = {'instances': [str(uuid.uuid4()) for i in range(NO_OF_INSTANCES)]}

dag = DAG('run_dag2', description='final running dag',
          schedule_interval=timedelta(seconds=120), start_date=datetime.now() - timedelta(days=1),
          # schedule_interval="@once",
          catchup=False, concurrency=20)

setup_task = SetupOperator(op_param={'instances': [str(uuid.uuid4()) for i in range(NO_OF_INSTANCES)]},
                           task_id='setup_task1', dag=dag, retries=3)

worker_task1 = WorkerOperator(op_param={"number": 1, "total": NO_OF_INSTANCES},
                              task_id='worker_task11', dag=dag)

collection_task1 = CollectionOperator(op_param={"number": 1, "total": NO_OF_INSTANCES},
                                      task_id='collection_task11', dag=dag)

worker_task2 = WorkerOperator(op_param={"number": 2, "total": NO_OF_INSTANCES},
                              task_id='worker_task21', dag=dag)

collection_task2 = CollectionOperator(op_param={"number": 2, "total": NO_OF_INSTANCES},
                                      task_id='collection_task21', dag=dag)

worker_task3 = WorkerOperator(op_param={"number": 3, "total": NO_OF_INSTANCES},
                              task_id='worker_task31', dag=dag)

collection_task3 = CollectionOperator(op_param={"number": 3, "total": NO_OF_INSTANCES},
                                      task_id='collection_task31', dag=dag)

completion_task = CompletionOperator(op_param=instance_info,
                                     task_id='completion_task1', dag=dag)

# setup_task >> worker_task1 >> collection_task1 >> completion_task
# setup_task >> worker_task2 >> collection_task2 >> completion_task
# setup_task >> worker_task3 >> collection_task3 >> completion_task
