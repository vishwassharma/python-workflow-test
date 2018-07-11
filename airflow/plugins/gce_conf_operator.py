import os
import time
import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

from taskers import xcom_pull, xcom_push, sync_folders, setup_instances, worker_task

from helper_functions import unzip, \
    delete_instances, download_blob_by_name, walktree_to_upload

from constants import *

log = logging.getLogger(__name__)


# TODO: Make these make these functions pluggable for the purpose of testing using docker

class SyncOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SyncOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        params = self.operator_param
        log.info("Sync in progress...")

        # the program must be running in the airflow home thus, airflow_home = current working directory
        ignores = ['.git', '/logs/', '.idea']
        sync_folders(upload_blob_name=DESTINATION_BLOB_NAME,
                     folder_root=os.getcwd(),
                     bucket_name=BUCKET_NAME,
                     ignores=ignores)
        log.info("Sync complete...")

        data = {
            "instance_info": params['instance_info'],
            "bin_data_source_blob": params['bin_data_source_blob'],
            "zip_blob": params['zip_blob'],
        }
        xcom_push(context, data)
        log.info("xcom data pushed: {}".format(data))


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
        xcom_data = xcom_pull(context, {
            'sync_task': ['instance_info', 'bin_data_source_blob', 'zip_blob'],
            'setup_task': 'created_instances',
            'unzip_task': 'status',
            'post_unzip_block_task': "started"
        })
        instance_info = xcom_data['sync_task']['instance_info']
        bin_data_source_blob = xcom_data['sync_task']['bin_data_source_blob']
        zip_blob = xcom_data['sync_task']['zip_blob']
        created_instances = xcom_data['setup_task']['created_instances'] or []
        unzip_status = xcom_data['unzip_task']['status']
        post_unzip_block_started = xcom_data['post_unzip_block_task']['started']
        log.info("xcom data received: {}".format(xcom_data))

        EXPORT_ENV_CONFIG_WORKER = [
            {
                "name": "NO_OF_INSTANCES",
                "value": len(instance_info['instances'])
            },
            {
                "name": "BIN_DATA_SOURCE_BLOB",
                "value": bin_data_source_blob
            },
            {
                "name": "ZIP_BLOB",
                "value": zip_blob
            }
        ]

        if unzip_status == True:
            create_instances = instance_info['instances']
            for instance in created_instances:
                if instance in create_instances:
                    create_instances.remove(instance)
            setup_instances(instances=create_instances, export_configs=EXPORT_ENV_CONFIG_WORKER)
            xcom_push(context, {'created_instances': created_instances + create_instances})
            if post_unzip_block_started == True:
                # return true if unzip is complete and the blocking of worker after unzipping has started
                return True
        else:
            if len(created_instances) >= 1:
                # instance is already
                return False
            create_instance = instance_info['instances'][0]
            setup_instances(instances=create_instance, export_configs=EXPORT_ENV_CONFIG_WORKER)
            xcom_push(context, {'created_instances': [create_instance]})

        return False


class UnzipOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(UnzipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        xcom_data = xcom_pull(context, {
            'sync_task': ['instance_info', 'zip_blob', 'bin_data_source_blob'],
            'setup_task': 'created_instances',
            'unzip_task': 'status'
        })
        zip_blob = xcom_data['sync_task']['zip_blob']
        bin_root_blob = xcom_data['sync_task']['bin_data_source_blob']
        log.info('xcom data received: {}'.format(xcom_data))

        file_paths = download_blob_by_name(source_blob_name=zip_blob, bucket_name=BUCKET_NAME,
                                           save_file_root=os.path.expanduser('~/zip_bin_log'))
        unzip_roots = []
        for path in file_paths:
            # unzipping the files
            unzip_root = unzip(path)
            os.remove(path)  # remove the file
            walktree_to_upload(tree_root=unzip_root,
                               root_blob=bin_root_blob, delete=True)  # TODO: Make this upload faster using parallel uploads
            unzip_roots.append(unzip_root)

        xcom_push(context, {'status': True})
        log.info("unzipping complete")


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
        xcom_push(context, {"started": True})
        xcom_data = xcom_pull(context, {
            'completion_task': ['started'],
        })
        completion_start = xcom_data['completion_task']['started']
        log.info("xcom_data: {}".format(xcom_data))
        if completion_start == True:
            return True
        return False


class WorkerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(WorkerOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("working")
        xcom_data = xcom_pull(context, {
            'sync_task': ['bin_data_source_blob']
        })
        bin_data_source_blob = xcom_data['sync_task']['bin_data_source_blob']
        log.info("xcom_data: {}".format(xcom_data))

        worker_task(instance_no=self.operator_param['number'],
                    total_instances=self.operator_param['total'],
                    bin_data_source_blob=bin_data_source_blob)
        xcom_push(context, {"status": True})


class CompletionOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(CompletionOperator, self).__init__(*args, **kwargs)

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
        #         task_instance.xcom_push(key='status', value=True)
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
        xcom_push(context, {"started": True})
        # result = super(CompletionOperator, self).poke(context)
        xcom_data = xcom_pull(context, {
            'sync_task': ['instance_info'],  # 'zip_blob'],
            # 'setup_task': 'created_instances',
            # 'unzip_task': 'status'
        })
        instance_info = xcom_data['sync_task']['instance_info']
        total_instance = len(instance_info['instances'])
        count = 0
        for i in range(total_instance):
            task_id = 'worker_task' + str(i)
            work_status = xcom_pull(context, {task_id: 'status'})[task_id]['status']
            if work_status == True:
                count += 1

        log.info("count: {}, total_instances: {} ".format(count, total_instance))
        if count == total_instance:
            xcom_push(context, {'status': True})
            result = True
        else:
            result = False

        if result:
            log.info("deleting instances")
            xcom_data = xcom_pull(context, {
                'sync_task': 'instance_info',
                'setup_task': 'created_instances',
                'unzip_task': 'status'
            })
            instance_info = xcom_data['sync_task']['instance_info']
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
        UnzipOperator,
        BlockSensorOperator,
        WorkerOperator,
        # WorkerBlockSensorOperator,
        CompletionOperator
    ]
