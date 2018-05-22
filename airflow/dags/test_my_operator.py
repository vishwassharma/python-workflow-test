# from datetime import datetime
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import MyFirstOperator
#
# dag = DAG('my_test_dag', description='Another tutorial DAG',
#           schedule_interval='0 12 * * *',
#           start_date=datetime(2017, 3, 20), catchup=False)
#
# dummy_task = DummyOperator(task_id='dummy_task', dag=dag)
#
# operator_task = MyFirstOperator(my_operator_param='This is a test.',
#                                 task_id='my_first_operator_task', dag=dag)
#
# dummy_task >> operator_task

from datetime import datetime
from airflow import DAG
from airflow.operators import UnzipOperator, FilterOperator, ProcessOperator, PublishOperator

print("________________________________________________")
dag = DAG('my_test_dag4', description='Another tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

unzip_task = UnzipOperator(op_param={"path": "/home/vishwas/output_ORIG.txt.zip"},
                           task_id='unzip_task', dag=dag, retries=3)

filter_task = FilterOperator(op_param={"input_file": "output_ORIG.txt",
                                        "output_file": "output_ORIG.txt_filtered"},
                             task_id="filter_task", dag=dag)

process_task = ProcessOperator(op_param={"input_file": "output_ORIG.txt_filtered",
                                          "output_file": "output_ORIG.txt_processed"},
                               task_id="process_task", dag=dag)

publish_task = PublishOperator(op_param={"input_file": "output_ORIG.txt_processed"},
                               task_id="publish_task", dag=dag)

# dummy_task >> sensor_task >> operator_task

unzip_task >> filter_task >> process_task >> publish_task


# from datetime import datetime
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import MyFirstOperator, MyFirstSensor
#
#
# dag = DAG('my_test_dag', description='Another tutorial DAG',
#           schedule_interval='0 12 * * *',
#           start_date=datetime(2017, 3, 20), catchup=False)
#
# dummy_task = DummyOperator(task_id='dummy_task', dag=dag)
#
# sensor_task = MyFirstSensor(task_id='my_sensor_task', poke_interval=30, dag=dag)
#
# operator_task = MyFirstOperator(my_operator_param='This is a test.',
#                                 task_id='my_first_operator_task', dag=dag)
#
# dummy_task >> sensor_task >> operator_task
