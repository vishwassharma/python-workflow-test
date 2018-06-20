import os
import time
import logging

from google.cloud import storage
from googleapiclient import discovery

from constants import *
import log_parser
from helper_functions import print_alias, \
    wait_for_operation, create_instance, delete_instance, \
    unzip, download_blob_by_name, walktree_to_upload, \
    assign_files, make_dirs, upload_blob, get_joinable_rear_path

log = logging.getLogger(__name__)


def xcom_push(context, data):
    """
    For pushing data to xcom
    :param context: context passed to the operator/sensor function
    :param data: <type: dict> data to be pushed
    :return: None
    """
    task_instance = context['ti']
    for key in data.keys():
        task_instance.xcom_push(key=key, value=data[key])


def xcom_pull(context, params):
    """
    For pulling data from xcom
    :param context: context passed to operator/sensor function
    :param params: <type: dict> Its structure should be like:
        {
            <task_id>: <key>,
            <task_id>: [<keys>,],
        }
    :return: <type:dict>
        {
            <task_id>:{
                          <key>: <value>,
            },
        }
    """
    data = {}
    task_instance = context['ti']
    # task_instance.xcom_pull(key='instance_info', task_ids='sync_task')
    for task_id in params.keys():
        data[task_id] = {}
        keys = params[task_id] if isinstance(params[task_id], (list, tuple)) else [params[task_id]]
        for key in keys:
            data[task_id][key] = task_instance.xcom_pull(key=key, task_ids=task_id)

    return data


def sync_folders(upload_blob_name, folder_root, bucket_name, ignores=None):
    """
    To sync the folders with the cloud storage for the compute instances to pull
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs()
    for blob in blob_list:
        if blob.name.__contains__(upload_blob_name):
            bucket.delete_blob(blob.name)
    walktree_to_upload(tree_root=folder_root, ignores=ignores,
                       bucket_name=bucket_name, root_blob=upload_blob_name)


def setup_instances(instances, export_configs):
    """
    will create instances, has to run on local/permanent machine
    """
    if not isinstance(instances, (list, tuple)):
        instances = [instances]
    project = PROJECT_NAME
    bucket = BUCKET_NAME
    zone = ZONE
    compute = discovery.build('compute', 'v1')
    for instance in instances:
        log.info('Creating instance.')
        operation = create_instance(compute, project, zone, instance, bucket, export_configs)
        wait_for_operation(compute, project, zone, operation['name'])
        log.info("instance {} created".format(instance))


def worker_task(instance_no, total_instances, bin_data_source_blob):
    """
    get the task for the worker
    arguments contains the various parameters that will
    be used by the machines to process the data like file numbers
    instance_no belongs to [0, total_instances - 1]

    :param instance_no: the instance_no, this process is running on
    :param total_instances: total no. of instances
    :param bin_data_source_blob: blob name of for binary data
    """
    if log:
        log_info = log.info
    else:
        log_info = print_alias

    BIN_DATA_STORAGE = os.path.expanduser('~/raw_data')  # binary will be stored in ~/raw_data
    PROCESSED_DATA_BLOB_NAME = "processed/" + bin_data_source_blob  # blob name for processed data
    PROCESSED_DATA_STORAGE = os.path.expanduser('~/' + PROCESSED_DATA_BLOB_NAME)  # processed data storage loc

    assigned_blobs = assign_files(instance_no=instance_no,
                                  total_instances=total_instances,
                                  bin_data_source_blob=bin_data_source_blob)
    log_info("Instance_no: {}".format(instance_no))
    log_info('Blobs assigned: ' + str(assigned_blobs))

    # downloading the files
    file_names = []
    for blob in assigned_blobs:  # downloading bin files
        rel_file_name = blob.name.replace(bin_data_source_blob, '')
        joinable_rel_file_name = get_joinable_rear_path(rel_file_name)
        filename = os.path.join(BIN_DATA_STORAGE, joinable_rel_file_name)   # absolute path for raw_data
        make_dirs(os.path.dirname(filename))
        blob.download_to_filename(filename)
        log_info('File {} downloaded to {}'.format(str(blob.name), filename))
        file_names.append(filename)

    save_names = []
    upload_names = []
    for filename in file_names:
        # processing the file
        save_filename = filename.replace(BIN_DATA_STORAGE, PROCESSED_DATA_STORAGE).replace('.bin', '.json')
        make_dirs(os.path.dirname(save_filename))
        log_parser.main(log, filename=filename, save_filename=save_filename)
        save_names.append(save_filename)

        # uploading the file
        upload_name = save_filename.replace(os.path.expanduser('~/'), '')
        upload_blob(source_file_path=save_filename,
                    destination_blob_name=upload_name, bucket_name=BUCKET_NAME)
        upload_names.append(upload_name)

    print ("file_names: {}".format(file_names))
    print ("save_names: {}".format(save_names))
    print ("upload_names: {}".format(upload_names))


def delete_instances(instances):
    """
    has to run on the local/permanent machine to destroy the instances after completion of work.
    """
    project = PROJECT_NAME
    zone = ZONE
    for instance in instances:
        compute = discovery.build('compute', 'v1')
        operation = delete_instance(compute, project, zone, instance)
        wait_for_operation(compute, project, zone, operation['name'])
        print("instance {} deleted...".format(instance))
