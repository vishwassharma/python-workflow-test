"""
This is module is for remote-distributed processing of data.

1) create and configure the instances
2) distribute the data to the instances
3) distribute the tasks
4) wait for result from instances
5) terminate the instances

"""

from os import path
import logging
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import time

from helper_functions import sync_folders, setup_instances, worker_task, collect_results, delete_instances

log = logging.getLogger(__name__)


class SetupOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(SetupOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("setting up")
        sync_folders()
        setup_instances()


class WorkerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(WorkerOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("working")

        worker_task()


class CollectionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(CollectionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("collecting data")
        collect_results()


class CompletionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(CompletionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("deleting instances")
        delete_instances()


class MyFirstPlugin(AirflowPlugin):
    name = "gce_plugin"
    operators = [SetupOperator, WorkerOperator, CollectionOperator, CompletionOperator]


# def sleep(seconds=0):
#     time.sleep(seconds)
#
#
# def unzip(path_to_zip):
#     import zipfile
#     zip_ref = zipfile.ZipFile(path_to_zip, 'r')
#     zip_ref.extractall(path.dirname(path_to_zip))
#     zip_ref.close()
#     return path.dirname(path_to_zip)
#
#
# def processor_func(data):
#     """
#     Do some processing
#     :param data:
#     :return: processed data
#     """
#
#     processed_data = data
#     return processed_data
#
#
# class UnzipOperator(BaseOperator):
#     @apply_defaults
#     def __init__(self, op_param, *args, **kwargs):
#         self.operator_param = op_param
#         super(UnzipOperator, self).__init__(*args, **kwargs)
#
#     def execute(self, context):
#         sleep(5)
#         log.info("Hello World!")
#         log.info('operator_param: %s', self.operator_param)
#         # print(context)
#         unzip(self.operator_param['path'])
#
#
# class FilterOperator(BaseOperator):
#     @apply_defaults
#     def __init__(self, op_param, *args, **kwargs):
#         self.operator_param = op_param
#         super(FilterOperator, self).__init__(*args, **kwargs)
#
#     def execute(self, context):
#         sleep(5)
#         log.info("Hello World!")
#         log.info('operator_param: %s', self.operator_param)
#         with open(self.operator_param['input_file']) as infile, \
#                 open(self.operator_param['output_file'], 'w') as outfile:
#             for line in infile.readlines():
#                 # print(line.split())
#                 if line.startswith('#'):
#                     continue
#                 elif line.split()[1] == '3':
#                     continue
#                 else:
#                     outfile.write(line)
#
#
# class ProcessOperator(BaseOperator):
#     @apply_defaults
#     def __init__(self, op_param, *args, **kwargs):
#         self.operator_param = op_param
#         super(ProcessOperator, self).__init__(*args, **kwargs)
#
#     def execute(self, context):
#         sleep(5)
#         log.info("Hello World!")
#         log.info('operator_param: %s', self.operator_param)
#         with open(self.operator_param['input_file']) as infile, \
#                 open(self.operator_param['output_file'], 'w') as outfile:
#             outfile.write(processor_func(infile.read()))
#
#
# class PublishOperator(BaseOperator):
#     @apply_defaults
#     def __init__(self, op_param, *args, **kwargs):
#         self.operator_param = op_param
#         super(PublishOperator, self).__init__(*args, **kwargs)
#
#     def execute(self, context):
#         sleep(5)
#         log.info("Hello World!")
#         log.info('operator_param: %s', self.operator_param)
#         with open(self.operator_param['input_file']) as infile:
#             print(infile.read())
