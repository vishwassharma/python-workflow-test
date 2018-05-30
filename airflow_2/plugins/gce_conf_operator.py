import time
import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

from helper_functions import sync_folders, setup_instances, worker_task, delete_instances

log = logging.getLogger(__name__)


class SyncOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SyncOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Sync in progress...")
        sync_folders()
        log.info("Instance info received: " + str(self.operator_param['instance_info']))
        task_instance = context['ti']
        task_instance.xcom_push(key='instance_info', value=self.operator_param['instance_info'])
        log.info("Sync complete...")


class SetupOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SetupOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("setting up")
        instance_info = context['ti'].xcom_pull(key='instance_info', task_ids='sync_task')
        log.info("Instance info received: " + str(instance_info))
        setup_instances(instances=instance_info['instances'])
        log.info("Instances created")


class SleepOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SleepOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("sleeping... Zzzz.....")
        time.sleep(5)


class BlockSensorOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(BlockSensorOperator, self).__init__(*args, **kwargs)

    def poke(self, context):
        task_instance = context['ti']
        instance_info = task_instance.xcom_pull(key='instance_info', task_ids='sync_task')
        log.info("Instance info received: " + str(instance_info))
        total_instance = len(instance_info['instances'])
        count = 0
        for i in range(total_instance):
            work_status = task_instance.xcom_pull(key='work', task_ids='worker_task' + str(i))

            # noinspection PySimplifyBooleanCheck
            if work_status == True:
                count += 1

        log.info("count: " + str(count))
        log.info("total_instance: " + str(total_instance))
        if count == total_instance:
            return True
        return False


class WorkerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(WorkerOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("working")
        task_instance = context['ti']
        instance_info = task_instance.xcom_pull(key='instance_info', task_ids='sync_task')
        log.info("Instances info received: " + str(instance_info))
        worker_task(logger=log, instance_no=self.operator_param['number'],
                    total_instances=self.operator_param['total'])
        log.info("dir(task_instance): " + str(dir(task_instance)))
        task_instance.xcom_push(key="work", value=True)


class CompletionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(CompletionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("deleting instances")
        instance_info = context['ti'].xcom_pull(key='instance_info', task_ids='sync_task')
        log.info("Instance info received: " + str(instance_info))
        delete_instances(instances=instance_info['instances'])
        log.info("Instances deleted")


class GcePlugin(AirflowPlugin):
    name = "gce_plugin"
    operators = [
        SyncOperator,
        SetupOperator,
        SleepOperator,
        BlockSensorOperator,
        WorkerOperator,
        CompletionOperator
    ]
