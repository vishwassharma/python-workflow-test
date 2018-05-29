"""
This is module is for remote-distributed processing of data.

1) create and configure the instances
2) distribute the data to the instances
3) distribute the tasks
4) wait for result from instances
5) terminate the instances

"""

import os
import time
import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from helper_functions import sync_folders, setup_instances, worker_task, collect_results, delete_instances

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


class SetupOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SetupOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("setting up")
        setup_instances(instances=self.operator_param['instances'])
        log.info("Instances created")


class WorkerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(WorkerOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("working")
        worker_task(instance_number=self.operator_param['number'],
                    total_instances=self.operator_param['total'])


class CollectionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(CollectionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("collecting data")
        collect_results(instance_number=self.operator_param['number'],
                        total_instances=self.operator_param['total'])


class CompletionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(CompletionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("deleting instances")
        delete_instances(instances=self.operator_param['instances'])


class GcePlugin(AirflowPlugin):
    name = "gce_plugin"
    operators = [SyncOperator, SetupOperator, WorkerOperator, CollectionOperator, CompletionOperator]
