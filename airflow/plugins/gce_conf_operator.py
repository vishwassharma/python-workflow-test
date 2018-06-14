import os
import time
import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

from helper_functions import unzip, sync_folders, setup_instances, worker_task, delete_instances, download_blob_by_name, \
    walktree_to_upload

log = logging.getLogger(__name__)


class SyncOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SyncOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Sync in progress...")
        sync_folders()
        log.info("Sync complete...")
        log.info("Instance info received: " + str(self.operator_param['instance_info']))
        log.info("bin_data_source_blob info received: " + str(self.operator_param['bin_data_source_blob']))
        task_instance = context['ti']
        task_instance.xcom_push(key='instance_info', value=self.operator_param['instance_info'])
        task_instance.xcom_push(key='bin_data_source_blob', value=self.operator_param['bin_data_source_blob'])
        task_instance.xcom_push(key='zip_blob', value=self.operator_param['zip_blob'])


# class SetupOperator(BaseOperator):
#     @apply_defaults
#     def __init__(self, op_param, *args, **kwargs):
#         self.operator_param = op_param
#         super(SetupOperator, self).__init__(*args, **kwargs)
#
#     def execute(self, context):
#         log.info("setting up")
#         task_instance = context['ti']
#         instance_info = task_instance.xcom_pull(key='instance_info', task_ids='sync_task')
#         log.info("Instance info received: " + str(instance_info))
#         setup_instances(instances=instance_info['instances'])
#         log.info("Instances created")

class SetupOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SetupOperator, self).__init__(*args, **kwargs)

    def poke(self, context):
        """
        creates an instance and pushes its id into xcom under 'created_instances` and returns False.
        check if the unzip instance has returned its status to be true in xcom and then creates the
        remaining instances.
        """
        task_instance = context['ti']
        instance_info = task_instance.xcom_pull(key='instance_info', task_ids='sync_task')
        created_instances = task_instance.xcom_pull(key='created_instances', task_ids='setup_task')
        unzip_status = task_instance.xcom_pull(key='status', task_ids='unzip_task')
        if unzip_status == True:
            create_instances = instance_info['instances']
            for instance in created_instances:
                if instance in create_instances:
                    create_instances.remove(instance)
            setup_instances(instances=create_instances)
            task_instance.xcom_push(key='created_instances', value=instance_info['instances'])
            return True
        else:
            create_instance = instance_info['instances'][0]
            if create_instance in created_instances:
                return False
            setup_instances(instances=create_instance)
            task_instance.xcom_push(key='created_instances', value=[create_instance])
            return False


class UnzipOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(UnzipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['ti']
        zip_blob = task_instance.xcom_pull(key='zip_blob', task_ids='sync_task')
        log.info('zip_blob: {}'.format(zip_blob))
        # Download the file in the instance and get its path.
        # feed this path to the unzip function
        # get path to unzipped folder and upload this folder
        file_paths = download_blob_by_name(zip_blob, os.path.expanduser('~'))
        unzipped_root = []
        for path in file_paths:
            # unzipping the files
            unzipped_root.append(unzip(path))
            walktree_to_upload()  # TODO: Make this upload faster using parallel uploads
        task_instance.xcom_push(key='status', value=True)


class SleepOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param=None, *args, **kwargs):
        if op_param is None:
            op_param = {'sleep_time': 0}
        self.operator_param = op_param
        super(SleepOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("sleeping... for {}. Zzzz.....".format(self.operator_param['sleep_time']))
        time.sleep(self.operator_param['sleep_time'])


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
            task_instance.xcom_push(key='complete', value=True)
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
        bin_data_source_blob = task_instance.xcom_pull(key='bin_data_source_blob', task_ids='sync_task')
        log.info("Instances info received: " + str(instance_info))
        log.info("bin_data_source_blob info received: " + str(bin_data_source_blob))
        worker_task(logger=log, instance_no=self.operator_param['number'],
                    total_instances=self.operator_param['total'],
                    bin_data_source_blob=bin_data_source_blob)
        log.info("dir(task_instance): " + str(dir(task_instance)))
        task_instance.xcom_push(key="work", value=True)


# class WorkerBlockSensorOperator(BaseSensorOperator):
#     @apply_defaults
#     def __init__(self, op_param, *args, **kwargs):
#         self.operator_param = op_param
#         super(WorkerBlockSensorOperator, self).__init__(*args, **kwargs)
#
#     def poke(self, context):
#         task_instance = context['ti']
#         block_status = task_instance.xcom_pull(key='complete', task_ids='blocking_task')
#         log.info("parent worker blocking task complete: " + str(block_status))
#         if block_status == True:
#             time.sleep(5)
#             return True
#         return False


class CompletionOperator(BlockSensorOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(CompletionOperator, self).__init__(op_param=op_param, *args, **kwargs)

        # def execute(self, context):
        #     task_instance = context['ti']
        #     instance_info = task_instance.xcom_pull(key='instance_info', task_ids='sync_task')
        #     log.info("Instance info received: " + str(instance_info))
        #     total_instance = len(instance_info['instances'])
        #     count = 0
        #     for i in range(total_instance):
        #         work_status = task_instance.xcom_pull(key='work', task_ids='worker_task' + str(i))
        #
        #         # noinspection PySimplifyBooleanCheck
        #         if work_status == True:
        #             count += 1
        #
        #     log.info("count: " + str(count))
        #     log.info("total_instance: " + str(total_instance))
        #     if count == total_instance:
        #         task_instance.xcom_push(key='complete', value=True)
        #         return True
        #     return False
        #
        #     # so that the workers complete their blocking task before termination and notifying the server
        #     # time.sleep(30)
        #     log.info("deleting instances")
        #     instance_info = context['ti'].xcom_pull(key='instance_info', task_ids='sync_task')
        #     log.info("Instance info received: " + str(instance_info))
        #     delete_instances(instances=instance_info['instances'])
        #     log.info("Instances deleted")

    def poke(self, context):
        result = super(CompletionOperator, self).poke(context)

        if result:
            log.info("deleting instances")
            instance_info = context['ti'].xcom_pull(key='instance_info', task_ids='sync_task')
            log.info("Instance info received: " + str(instance_info))
            delete_instances(instances=instance_info['instances'])
            log.info("Instances deleted")

        return result


class GcePlugin(AirflowPlugin):
    name = "gce_plugin"
    operators = [
        SyncOperator,
        SetupOperator,
        SleepOperator,
        # BlockSensorOperator,
        WorkerOperator,
        # WorkerBlockSensorOperator,
        CompletionOperator
    ]
