import uuid
from datetime import datetime, timedelta
import airflow
from airflow.operators import (SyncOperator,
                               SetupOperator,
                               BlockSensorOperator,
                               WorkerOperator,
                               CollectionOperator,
                               CompletionOperator, )

# TODO: remove the hard-coding for three instances and make it dynamic

NO_OF_INSTANCES = 3
instance_info = {'instances': [str(uuid.uuid4()).replace("-", "") for i in range(NO_OF_INSTANCES)]}
# instance_info = {'instances': ['test1']}

def_args = {
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}

dag = airflow.DAG('run_dag5', description='final running dag',
                  schedule_interval=timedelta(seconds=7200),
                  # start_date=datetime.now() - timedelta(days=1),
                  # schedule_interval="@once",
                  catchup=False, concurrency=20, default_args=def_args)

sync_task = SyncOperator(op_param={'instance_info': instance_info},
                         task_id='sync_task', dag=dag)

setup_task = SetupOperator(op_param={},
                           task_id='setup_task1', dag=dag)

blocking_task = BlockSensorOperator(op_param={},
                                    task_id='blocking_task', dag=dag)

sync_task >> setup_task >> blocking_task


worker_tasks = []

for instance_no in range(len(instance_info['instances'])):
    worker_tasks.append(WorkerOperator(op_param={"number": instance_no, "total": NO_OF_INSTANCES},
                                       task_id='worker_task' + str(instance_no), dag=dag))

completion_task = CompletionOperator(op_param=instance_info,
                                     task_id='completion_task1', dag=dag)

for wTask in worker_tasks:
    setup_task >> wTask >> completion_task


# worker_task1 = WorkerOperator(op_param={"number": 0, "total": NO_OF_INSTANCES},
#                               task_id='worker_task11', dag=dag)
#
# collection_task1 = CollectionOperator(op_param={"number": 0, "total": NO_OF_INSTANCES},
#                                       task_id='collection_task11', dag=dag)
#
# # worker_task2 = WorkerOperator(op_param={"number": 1, "total": NO_OF_INSTANCES},
# #                               task_id='worker_task21', dag=dag)
# #
# # collection_task2 = CollectionOperator(op_param={"number": 1, "total": NO_OF_INSTANCES},
# #                                       task_id='collection_task21', dag=dag)
# #
# # worker_task3 = WorkerOperator(op_param={"number": 2, "total": NO_OF_INSTANCES},
# #                               task_id='worker_task31', dag=dag)
# #
# # collection_task3 = CollectionOperator(op_param={"number":2, "total": NO_OF_INSTANCES},
# #                                       task_id='collection_task31', dag=dag)
# sync_task >> setup_task >> worker_task1 >> collection_task1 >> completion_task
# setup_task >> worker_task2 >> collection_task2 >> completion_task
# setup_task >> worker_task3 >> collection_task3 >> completion_task
