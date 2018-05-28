from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import UnzipOperator, FilterOperator, ProcessOperator, PublishOperator

dag = DAG('my_test_dag0', description='Another tutorial DAG',
          schedule_interval=timedelta(seconds=300),
          start_date=datetime.now().date() - timedelta(days=1), catchup=False, concurrency=20)

unzip_task = UnzipOperator(op_param={"path": "/home/vishwas/Downloads/output_ORIG.txt.zip"},
                           task_id='unzip_task', dag=dag, retries=3)

filter_task = FilterOperator(op_param={"input_file": "/home/vishwas/Downloads/output_ORIG.txt",
                                       "output_file": "output_ORIG.txt_filtered"},
                             task_id="filter_task", dag=dag)

process_task = ProcessOperator(op_param={"input_file": "output_ORIG.txt_filtered",
                                         "output_file": "output_ORIG.txt_processed"},
                               task_id="process_task", dag=dag)

publish_task = PublishOperator(op_param={"input_file": "output_ORIG.txt_processed"},
                               task_id="publish_task", dag=dag)

unzip_task >> filter_task >> process_task >> publish_task

# from airflow.operators import SetupOperator, WorkerOperator, CollectionOperator, CompletionOperator
#
# dag = DAG('run_dag1', description='final running dag',
#           schedule_interval=timedelta(days=1),
#           start_date=datetime(2018, 1, 1), catchup=False, concurrency=20)
#
# # unzip_task = UnzipOperator(op_param={"path": "/home/vishwas/Downloads/output_ORIG.txt.zip"},
# #                            task_id='unzip_task', dag=dag, retries=3)
# #
# # filter_task = FilterOperator(op_param={"input_file": "/home/vishwas/Downloads/output_ORIG.txt",
# #                                        "output_file": "output_ORIG.txt_filtered"},
# #                              task_id="filter_task", dag=dag)
# #
# # process_task = ProcessOperator(op_param={"input_file": "output_ORIG.txt_filtered",
# #                                          "output_file": "output_ORIG.txt_processed"},
# #                                task_id="process_task", dag=dag)
# #
# # publish_task = PublishOperator(op_param={"input_file": "output_ORIG.txt_processed"},
# #                                task_id="publish_task", dag=dag)
#
# # unzip_task >> filter_task >> process_task >> publish_task
#
#
# setup_task = SetupOperator(op_param={},
#                            task_id='setup_task', dag=dag, retries=3)
#
# worker_task1 = WorkerOperator(op_param={},
#                               task_id='worker_task1', dag=dag)
#
# collection_task1 = CollectionOperator(op_param={},
#                                       task_id='collection_task1', dag=dag)
# #
# # worker_task2 = WorkerOperator(op_param={},
# #                               task_id='worker_task2', dag=dag)
# #
# # collection_task2 = CollectionOperator(op_param={},
# #                                       task_id='collection_task2', dag=dag)
# #
# # worker_task3 = WorkerOperator(op_param={},
# #                               task_id='worker_task3', dag=dag)
# #
# # collection_task3 = CollectionOperator(op_param={},
# #                                       task_id='collection_task3', dag=dag)
#
# completion_task = CompletionOperator(op_param={},
#                                      task_id='completion_task', dag=dag)
#
# setup_task >> worker_task1 >> collection_task1 >> completion_task
# # setup_task >> worker_task2 >> collection_task2 >> completion_task
# # setup_task >> worker_task3 >> collection_task3 >> completion_task
