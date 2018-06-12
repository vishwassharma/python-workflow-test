import os
import time
from stat import *

import dotenv
from google.cloud import storage
from googleapiclient import discovery

import log_parser

# from airflow.constants import DESTINATION_BLOB_NAME
DESTINATION_BLOB_NAME = 'airflow_home'
BINARY_FILE_BLOB_NAME = 'bin_log'
PROCESSED_DATA_BLOB_NAME = 'json_log'


def wait_for_operation(compute, project, zone, operation):
    """
    Waits for an Google cloud function to complete
    """
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)


def create_instance(compute, project, zone, name, bucket):
    """
    Creates a compute instance on the google cloud platform
    """
    # Get the latest Ubuntu 16.04 image.
    image_response = compute.images().getFromFamily(
        project='ubuntu-os-cloud', family='ubuntu-1604-lts').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    # TODO: gce_conf_script path hardcoded
    startup_script = open(os.path.join(os.path.dirname(__file__), 'gce_conf_script.sh'), 'r').read()
    # image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    # image_caption = "Ready for dessert?"

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }, {
                'key': 'bucket',
                'value': bucket
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()


def delete_instance(compute, project, zone, name):
    """
    Terminates an instance from the google cloud platform
    """
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()


def list_instances(compute, project, zone):
    """
    list all the active instances
    """
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items']


def sleep(seconds=0):
    """
    function for sleep
    """
    time.sleep(seconds)


def make_dirs(path):
    """
    Checks for a path it is exists if not, it creates one
    """
    if os.path.exists(path):
        return
    else:
        make_dirs(os.path.dirname(path))
        os.mkdir(path)


def upload_blob(bucket_name=os.environ.get("BUCKET_NAME", ""),
                source_file_name=os.environ.get("AIRFLOW_HOME", ""),
                destination_blob_name=DESTINATION_BLOB_NAME):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


# TODO: Not useful at the moment
# def download_blob_by_name(bucket_name=os.environ.get("BUCKET_NAME", ""), source_blob_name=DESTINATION_BLOB_NAME):
#     """Uploads a file to the bucket."""
#     # TODO: Check here for source of downloading and for path where its saved
#     storage_client = storage.Client()
#     bucket = storage_client.get_bucket(bucket_name)
#     blobs = bucket.list_blobs()
#     for blob in blobs:
#         if blob.name.startswith(source_blob_name):
#             # download the blob (file in its folder)
#             file_path = blob.name.replace(source_blob_name, "")
#             make_dirs(os.path.dirname(file_path))  # for creating the path recursively
#             blob.download_to_filename(file_path)


def walktree_to_upload(top=os.environ.get("AIRFLOW_HOME", "/home/rtheta/airflow"), callback=upload_blob):
    """
    recursively descend the directory tree rooted at top,
    calling the callback function for each regular file
    """
    # TODO: contains some hardcoding in the path names. This is internal to the project folder
    if top.__contains__(".git") or top.__contains__("/logs/") or top.__contains__(".idea"):
        return
    for f in os.listdir(top):
        pathname = os.path.join(top, f)
        mode = os.stat(pathname)[ST_MODE]
        if S_ISDIR(mode):
            # It's a directory, recurse into it
            walktree_to_upload(pathname, callback)
        elif S_ISREG(mode):
            if f.endswith(".pyc") or f.endswith('.env'):  # or f.startswith(".idea"):
                continue
            # It's a file, call the callback function
            file_blob_name = os.path.join(DESTINATION_BLOB_NAME,
                                          pathname.replace(os.environ.get("AIRFLOW_HOME", "") + "/", ""))
            # The BLOB_NAME will act as the airflow home directory
            callback(os.environ.get("BUCKET_NAME", ""), pathname, file_blob_name)
        else:
            # Unknown file type, print a message
            print('Skipping %s' % pathname)


def assign_files(instance_no, total_instances):
    """
    assigns files to the instances
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(os.environ.get("BUCKET_NAME", ""))
    # # for listing the blobs
    blobs_iter = bucket.list_blobs()
    blob_list = list(blobs_iter)

    req_blob = []
    for blob in blob_list:
        if blob.name.__contains__(BINARY_FILE_BLOB_NAME):
            req_blob.append(blob)

    q = len(req_blob) // total_instances
    r = len(req_blob) % total_instances

    start = instance_no * q + (instance_no if r - instance_no > 0 else r)
    end = start + q + (1 if r - instance_no > 0 else 0)
    """
    To test the validity for file distribution algorithm
    # instances = 11
    # files = 60
    q = 5
    r = 5
    for instance_no in range(11):
        start = instance_no * q + (instance_no if r - instance_no > 0 else r)
        end = start + q + (1 if r - instance_no > 0 else 0)
        print("start: {}, end: {}, total files: {}". format(start, end, end-start))
    """
    return [blob for blob in req_blob[start:end]]


def sync_folders(blob_name=DESTINATION_BLOB_NAME):
    """
    To sync the folders with the cloud storage for the instances to pull
    """
    # sleep()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(os.environ.get("BUCKET_NAME", ""))
    blob_list = bucket.list_blobs()
    for blob in blob_list:
        if blob.name.__contains__(blob_name):
            bucket.delete_blob(blob.name)
    walktree_to_upload()


def setup_instances(instances):
    """
    will create instances, has to run on local/permanent machine
    """
    sleep()
    project = os.environ.get("PROJECT_NAME", "")
    bucket = os.environ.get("BUCKET_NAME", "")
    zone = os.environ.get("ZONE", "")
    compute = discovery.build('compute', 'v1')
    for instance in instances:
        print('Creating instance.')
        operation = create_instance(compute, project, zone, instance, bucket)
        wait_for_operation(compute, project, zone, operation['name'])
        print("instance {} created".format(instance))


def worker_task(instance_no, total_instances, logger=None, *args, **kwargs):
    """
    get the task for the worker
    arguments contains the various parameters that will
    be used by the machines to process the data like file numbers
    """
    dotenv.load_dotenv("./.worker_env")  # TODO: find a way around

    BIN_DATA_STORAGE = os.path.expanduser('~/raw_data')
    PROCESSED_DATA_STORAGE = os.path.expanduser('~/' + PROCESSED_DATA_BLOB_NAME)

    assigned_blobs = assign_files(instance_no=instance_no, total_instances=total_instances)
    if logger:
        logger.info('Blobs assigned: ' + str(assigned_blobs))
    else:
        print 'Blobs assigned: ' + str(assigned_blobs)

    file_names = []
    for blob in assigned_blobs:  # downloading bin files
        filename = os.path.join(BIN_DATA_STORAGE + blob.name.replace(BINARY_FILE_BLOB_NAME + '/', ''))
        make_dirs(os.path.dirname(filename))
        blob.download_to_filename(filename)
        logger.info('File {} downloaded to {}'.format(str(blob.name), filename))
        file_names.append(filename)

    save_filenames = []
    for filename in file_names:
        save_filename = filename.replace(BIN_DATA_STORAGE, PROCESSED_DATA_STORAGE).replace('.bin', '.json')
        make_dirs(os.path.dirname(save_filename))
        log_parser.main(logger, filename=filename, save_filename=save_filename)
        save_filenames.append(save_filename)

    for save_filename in save_filenames:
        upload_name = save_filename.replace(os.path.expanduser('~/'), '')
        upload_blob(source_file_name=save_filename,
                    destination_blob_name=upload_name)


def delete_instances(instances):
    """
    has to run on the local/permanent machine to destroy the instances after completion of work.
    """
    sleep()
    project = os.environ.get("PROJECT_NAME", "")
    zone = os.environ.get("ZONE", "")
    for instance in instances:
        compute = discovery.build('compute', 'v1')
        operation = delete_instance(compute, project, zone, instance)
        wait_for_operation(compute, project, zone, operation['name'])
        print("instance {} deleted...".format(instance))


if __name__ == "__main__":
    # def pr(*args):
    #     print args
    #
    #
    # os.environ['AIRFLOW_HOME'] = "/home/rtheta/parser_pipeline/airflow"
    # print DESTINATION_BLOB_NAME
    # walktree_to_upload("/home/rtheta/parser_pipeline/airflow", pr)
    sync_folders()
