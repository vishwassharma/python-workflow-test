from os import path
import logging
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import time

log = logging.getLogger(__name__)


def sleep(seconds=0):
    time.sleep(seconds)


def unzip(path_to_zip):
    import zipfile
    zip_ref = zipfile.ZipFile(path_to_zip, 'r')
    zip_ref.extractall(path.dirname(path_to_zip))
    zip_ref.close()
    return path.dirname(path_to_zip)


def processor_func(data):
    """
    Do some processing
    :param data:
    :return: processed data
    """

    processed_data = data
    return processed_data


class UnzipOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(UnzipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # sleep(5)
        log.info("Running Unzip Operator")
        log.info("Hello World!")
        log.info('operator_param: %s', self.operator_param)
        # # print(context)
        # unzip(self.operator_param['path'])


class FilterOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(FilterOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # sleep(5)
        log.info("Running Filter Operator")
        log.info("Hello World!")
        log.info('operator_param: %s', self.operator_param)
        # with open(self.operator_param['input_file']) as infile, \
        #         open(self.operator_param['output_file'], 'w') as outfile:
        #     for line in infile.readlines():
        #         # print(line.split())
        #         if line.startswith('#'):
        #             continue
        #         elif line.split()[1] == '3':
        #             continue
        #         else:
        #             outfile.write(line)


class ProcessOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(ProcessOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # sleep(5)
        log.info("Running Process Operator")
        log.info("Hello World!")
        log.info('operator_param: %s', self.operator_param)
        # with open(self.operator_param['input_file']) as infile, \
        #         open(self.operator_param['output_file'], 'w') as outfile:
        #     outfile.write(processor_func(infile.read()))


class PublishOperator(BaseOperator):
    @apply_defaults
    def __init__(self, op_param, *args, **kwargs):
        self.operator_param = op_param
        super(PublishOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # sleep(5)
        log.info("Running Publish Operator")
        log.info("Hello World!")
        log.info('operator_param: %s', self.operator_param)
        # with open(self.operator_param['input_file']) as infile:
        #     print(infile.read())


class MyFirstPlugin(AirflowPlugin):
    name = "my_first_plugin"
    operators = [UnzipOperator, FilterOperator, ProcessOperator, PublishOperator]
