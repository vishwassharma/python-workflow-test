import time
import uuid
from googleapiclient import discovery
import argparse
import os
import time

import log_parser

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/rtheta/PycharmProjects/rtheta_learning/auth.ansible.json'


def wait_for_operation(compute, project, zone, operation):
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
    # Get the latest Debian Jessie image.
    image_response = compute.images().getFromFamily(
        # project='debian-cloud', family='debian-8').execute()
        project='ubuntu-os-cloud', family='ubuntu-1604-lts').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'gce_conf_script.sh'), 'r').read()
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

    # # TODO: try to do this in more secure way... -_-
    # auth_file = open(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
    #
    # auth_command = "echo \'" + auth_file.read() + "\' >> auth.ansible.json \n"
    # startup_script = auth_command + startup_script

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
                'key': 'url',
                'value': image_url
            }, {
                'key': 'text',
                'value': image_caption
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
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()


def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items']


# def main(project, bucket, zone, instance_name, wait=True):
#     compute = discovery.build('compute', 'v1')
#
#     print('Creating instance.')
#
#     operation = create_instance(compute, project, zone, instance_name, bucket)
#     wait_for_operation(compute, project, zone, operation['name'])
#
#     instances = list_instances(compute, project, zone)
#
#     print('Instances in project %s and zone %s:' % (project, zone))
#     for instance in instances:
#         print(' - ' + instance['name'])
#
#     print("""
# Instance created.
# It will take a minute or two for the instance to complete work.
# Check this URL: http://storage.googleapis.com/{}/output.png
# Once the image is uploaded press enter to delete the instance.
# """.format(bucket))
#
#     if wait:
#         input("Press any key to delete the instance...")
#
#     print('Deleting instance.')
#
#     operation = delete_instance(compute, project, zone, instance_name)
#     wait_for_operation(compute, project, zone, operation['name'])


def sleep(seconds=0):
    time.sleep(seconds)


import os
from stat import *
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/vishwas/PycharmProjects/rtheta_learning/auth.ansible.json'


def make_dirs(path):
    if os.path.exists(path):
        return
    else:
        make_dirs(os.path.dirname(path))
        os.mkdir(path)


def upload_blob(bucket_name='central.rtheta.in',
                source_file_name='/home/rtheta/PycharmProjects/rtheta_learning/airflow_2',
                destination_blob_name='folder_sync'):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # for listing the blobs
    # blobs = bucket.list_blobs()
    # for blob in blobs:
    #     print(blob.name)
    #     if blob.name.__contains__("airflow.cfg"):
    #         blob.download_to_filename('/home/rtheta/PycharmProjects/rtheta_learning/others/blah22')
    #         print("file downloaded")

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    # print('File {} uploaded to {}.'.format(
    #     source_file_name,
    #     destination_blob_name))


def download_blob(bucket_name='central.rtheta.in', source_blob_name='folder_sync'):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        if blob.name.startswith(source_blob_name):
            # download the blob (file in its folder)
            file_path = blob.name.replace(source_blob_name, "")
            make_dirs(os.path.dirname(file_path))  # for creating the path recursively
            blob.download_to_filename(file_path)


def walktree_to_upload(top='/home/rtheta/PycharmProjects/rtheta_learning/airflow_2', callback=upload_blob):
    """
    recursively descend the directory tree rooted at top,
       calling the callback function for each regular file
    """
    # print(top)

    if top.__contains__(".git") or top.__contains__("/logs/") or top.__contains__(".idea"):
        return
    for f in os.listdir(top):
        pathname = os.path.join(top, f)
        mode = os.stat(pathname)[ST_MODE]
        if S_ISDIR(mode):
            # It's a directory, recurse into it
            walktree_to_upload(pathname, callback)
        elif S_ISREG(mode):
            if f.endswith(".pyc"):  # or f.startswith(".idea"):
                continue
            # It's a file, call the callback function
            callback('central.rtheta.in', pathname, 'folder_sync/' + pathname)
            # callback(pathname)
        else:
            # Unknown file type, print a message
            print('Skipping %s' % pathname)


# def visitfile(file):              # testing function for walktree function
#     print('visiting', file)


def assign_files(instance_no, total_instances):
    """
    assigns files to the instances
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('central.rtheta.in')
    # # for listing the blobs
    blobs_iter = bucket.list_blobs()
    blob_list = list(blobs_iter)

    req_blob = []
    for blob in blob_list:
        if blob.name.__contains__('bin_log'):
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


def sync_folders(*args, **kwargs):
    """
    To sync the folders with the cloud storage for the instances to pull
    """
    sleep()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('central.rtheta.in')
    blob_list = bucket.list_blobs()
    for blob in blob_list:
        if blob.name.__contains__('folder_sync'):
            bucket.delete_blob(blob.name)
    walktree_to_upload()


def setup_instances(instances, *args, **kwargs):
    """
    will create instances, has to run on local/permanent machine
    """
    sleep()
    project = 'rtheta-central'
    bucket = 'central.rtheta.in'
    zone = 'us-central1-a'
    compute = discovery.build('compute', 'v1')
    for instance in instances:
        # instance = uuid.uuid4()

        print('Creating instance.')

        operation = create_instance(compute, project, zone, instance, bucket)
        wait_for_operation(compute, project, zone, operation['name'])
        print("instance {} created".format(instance))
        # instances = list_instances(compute, project, zone)

    #     print('Instances in project %s and zone %s:' % (project, zone))
    #     for instance in list_instances:
    #         print(' - ' + instance['name'])
    #
    #     print("""
    # Instance created.
    # It will take a minute or two for the instance to complete work.
    # Check this URL: http://storage.googleapis.com/{}/output.png
    # Once the image is uploaded press enter to delete the instance.
    #     """.format(bucket))


def worker_task(instance_no, total_instances, logger=None, *args, **kwargs):
    """
    get the task for the worker
    arguments contains the various parameters that will
    be used by the machines to process the data like file numbers
    """
    sleep()
    # TODO: download the files in all the workers or download the files on go as the worker iterates the list
    assigned_blobs = assign_files(instance_no=instance_no, total_instances=total_instances)
    for blob in assigned_blobs:
        log_parser.main(logger, filename=blob.name)     # TODO: pass a proper storage name or location


def collect_results(*args, **kwargs):
    """
    no need of this task :P. This all can be handled in worker_task only :sweat_sweat_smile:
    """
    sleep()
    pass


def delete_instances(instances, *args, **kwargs):
    """
    has to run on the local/permanent machine to destroy the instances after completion of work.
    """
    sleep()
    project = 'rtheta-central'
    bucket = 'central.rtheta.in'
    zone = 'us-central1-a'
    # instance_name = 'test1'
    for instance in instances:
        compute = discovery.build('compute', 'v1')
        operation = delete_instance(compute, project, zone, instance)
        wait_for_operation(compute, project, zone, operation['name'])
        print("instance {} deleted...".format(instance))
