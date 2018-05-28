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
