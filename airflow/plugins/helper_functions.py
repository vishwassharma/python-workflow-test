import os
import time
from stat import *

from google.cloud import storage
from googleapiclient import discovery

import log_parser
from constants import *

os.environ['PROJECT_NAME'] = "rtheta-central"
os.environ['BUCKET_NAME'] = "central.rtheta.in"
os.environ['ZONE'] = "asia-south1-a"

DESTINATION_BLOB_NAME = 'airflow_home'


def print_alias(*args):
    print(args)


def get_joinable_rear_path(path):
    """
    Returns the path that can be used as the later part in os.path.join() function
    :param path: original path
    :return: path not starting with `/`
    """
    return path if not path.startswith('/') else get_joinable_rear_path(path[1:])


def unzip(path_to_file):
    extract_location = os.path.dirname(path_to_file)
    if path_to_file.endswith("zip"):
        import zipfile
        zip_ref = zipfile.ZipFile(path_to_file, 'r')
        zip_ref.extractall(extract_location)
        zip_ref.close()
    elif path_to_file.endswith('tar') or path_to_file.endswith('tar.gz'):
        import tarfile
        if path_to_file.endswith("tar.gz"):
            tar = tarfile.open(path_to_file, "r:gz")
            tar.extractall(extract_location)
            tar.close()
        elif path_to_file.endswith("tar"):
            tar = tarfile.open(path_to_file, "r:")
            tar.extractall(extract_location)
            tar.close()
    else:
        raise Exception("Unknown file format")
    return extract_location


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


def parse_configs_to_command(config_list):
    config_export_command = "export {name}={value}\n"

    parsed_configs = ""
    for config in config_list:
        parsed_configs = parsed_configs + config_export_command.format(name=config.get('name'),
                                                                       value=config.get('value'))
    return parsed_configs


def get_airflow_configs():
    try:
        # TODO: DATABASE Schema for configs
        """
        The configs should be in format:
        {
            application: 'airflow'
            envs = [{
                "name": "AIRFLOW_HOME",
                "value": "/home/user/airflow"
            }, {
                "name": "AIRFLOW_LE",
                "value": "3232"
            }],
            queues: {
            
                type: 'redis'
                host: '',
                port: '',
                ..
            },
            inputs = {
                    "NO_OF_INSTANCES": <int>,
                    "BIN_DATA_SOURCE_BLOB": <str> root blob name for the binary files
                    "ZIP_BLOB": <str> prefix of the root directory of bin files without trailing `/`
            }
        }
        """
        from pymongo import MongoClient

        MONGO_HOST = '172.17.0.1/'
        # MONGO_HOST = '127.0.0.1'

        client = MongoClient(host=MONGO_HOST)
        # db = client['airflow_db']
        # collection = db['settings']
        # configs = list(collection.find())[-1].get('envs', [])
        # config_export_command = "export {name}={value}\n"
        #
        # exported_configs = ""
        # for config in configs:
        #     exported_configs = exported_configs + config_export_command.format(name=config.get('name'),
        #                                                                        value=config.get('value'))
        db = client['airflow_db']
        collection = db['settings']
        latest_document = list(collection.find())[-1]
        configs = latest_document['envs']
        exported_configs = parse_configs_to_command(config_list=configs)
    except Exception as e:
        print(e)
        exported_configs = ""

    return exported_configs


def create_instance(compute, project, zone, name, bucket, export_configs):
    """
    Creates a compute instance on the google cloud platform
    """
    # Get the latest Ubuntu 16.04 image.
    image_response = compute.images().getFromFamily(
        project='ubuntu-os-cloud', family='ubuntu-1604-lts').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone

    startup_script = open(os.path.join(os.path.dirname(__file__), 'gce_conf_script.sh'), 'r').read()
    become_superuser = "#!/usr/bin/env bash\n" + "sudo su\n"  # this is done here  because after changing the user, all the environment variables are gone
    exported_configs = parse_configs_to_command(export_configs)
    temp_string = "export AIRFLOW_HOME=" + os.getcwd() + '\n'
    overrided_configs = get_airflow_configs()

    startup_script = become_superuser + exported_configs + temp_string + overrided_configs + startup_script

    # startup_script.format(AIRFLOW_HOME=os.getcwd())
    print ("cwd: " + os.getcwd())
    print ("startup_script")
    print (startup_script)
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


def upload_blob(source_file_path, destination_blob_name=None, bucket_name=BUCKET_NAME,
                tree_root=None, root_blob=None, *args, **kwargs):
    """Uploads a file to the bucket"""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    if destination_blob_name is None:
        """ tree_root and root_blob should be present """
        tree_root = os.path.abspath(tree_root)  # path that must be excluded from file name before uploading
        # root_blob = kwargs['root_blob']  # the root blob where files must
        file_path = os.path.abspath(source_file_path)
        rel_file_path = file_path.replace(tree_root, "")  # file path relative to the tree_root
        destination_blob_name = os.path.join(root_blob,
                                             get_joinable_rear_path(rel_file_path))  # destination blob to upload file

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)


def download_blob_by_name(source_blob_name, bucket_name, save_file_root=""):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    file_paths = []
    for blob in blobs:
        if blob.name.__contains__(source_blob_name):
            file_name = blob.name.replace(source_blob_name, "")
            valid_file_name = get_joinable_rear_path(file_name)
            if valid_file_name == "":
                continue
            file_path = os.path.join(save_file_root, valid_file_name)
            make_dirs(os.path.dirname(file_path))  # for creating the path recursively
            blob.download_to_filename(file_path)
            file_paths.append(file_path)
    return file_paths


# def walktree_to_upload(top=os.environ.get("AIRFLOW_HOME", os.getcwd()), callback=upload_blob):
def walktree_to_upload(tree_root, cur_dir=None, callback=upload_blob, ignores=None, *args, **kwargs):
    """
    recursively descend the directory tree rooted at top,
    calling the callback function for each regular file
    """
    if not cur_dir:
        cur_dir = tree_root
    if ignores is not None:
        for path in ignores:
            if cur_dir.__contains__(path):
                return
    # if top.__contains__(".git") or top.__contains__("/logs/") or top.__contains__(".idea"):
    #     return
    for f in os.listdir(cur_dir):
        pathname = os.path.join(cur_dir, f)
        mode = os.stat(pathname)[ST_MODE]
        if S_ISDIR(mode):
            # It's a directory, recurse into it
            walktree_to_upload(tree_root=tree_root, cur_dir=pathname, callback=callback, *args, **kwargs)
        elif S_ISREG(mode):
            if f.endswith(".pyc") or f.endswith('.env'):  # or f.startswith(".idea"):
                continue
            # It's a file, call the callback function
            # blob_name_rear = pathname.replace(absolute_root, "")
            # file_blob_name = os.path.join(blob_name, get_joinable_rear_path(blob_name_rear))
            # The BLOB_NAME will act as the airflow home directory
            # callback(pathname, file_blob_name)
            callback(tree_root=tree_root, source_file_path=pathname, *args, **kwargs)
        else:
            # Unknown file type, print a message
            print('Skipping %s' % pathname)


def assign_files(instance_no, total_instances, bin_data_source_blob):
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
        if blob.name.startswith(bin_data_source_blob):
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
    if not isinstance(instances, list):
        instances = [instances]
    project = os.environ.get("PROJECT_NAME", "")
    bucket = os.environ.get("BUCKET_NAME", "")
    zone = os.environ.get("ZONE", "")
    compute = discovery.build('compute', 'v1')
    for instance in instances:
        print('Creating instance.')
        operation = create_instance(compute, project, zone, instance, bucket)
        wait_for_operation(compute, project, zone, operation['name'])
        print("instance {} created".format(instance))


def worker_task(instance_no, total_instances, bin_data_source_blob, logger=None):
    """
    get the task for the worker
    arguments contains the various parameters that will
    be used by the machines to process the data like file numbers
    instance_no belongs to [0, total_instances - 1]
    """
    if logger:
        log_info = logger.info
    else:
        log_info = print_alias

    BIN_DATA_STORAGE = os.path.expanduser('~/raw_data')
    PROCESSED_DATA_BLOB_NAME = "processed/" + bin_data_source_blob
    PROCESSED_DATA_STORAGE = os.path.expanduser('~/' + PROCESSED_DATA_BLOB_NAME)

    assigned_blobs = assign_files(instance_no=instance_no,
                                  total_instances=total_instances,
                                  bin_data_source_blob=bin_data_source_blob)
    log_info("Instance_no: {}".format(instance_no))
    log_info('Blobs assigned: ' + str(assigned_blobs))

    # downloading the files
    file_names = []
    for blob in assigned_blobs:  # downloading bin files
        rel_file_name = blob.name.replace(bin_data_source_blob + '/', '')
        filename = os.path.join(BIN_DATA_STORAGE, rel_file_name)
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
        log_parser.main(logger, filename=filename, save_filename=save_filename)
        save_names.append(save_filename)

        # uploading the file
        upload_name = save_filename.replace(os.path.expanduser('~/'), '')
        upload_blob(source_file_name=save_filename,
                    destination_blob_name=upload_name)
        upload_names.append(upload_name)

        # print ("file_names: {}".format(file_names))
        # print ("save_names: {}".format(save_names))
        # print ("upload_names: {}".format(upload_names))


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

# if __name__ == "__main__":
#     # def pr(*args):
#     #     print (args)
#     #
#     #
#     # os.environ['AIRFLOW_HOME'] = "/home/rtheta/parser_pipeline/airflow"
#     # print (DESTINATION_BLOB_NAME)
#     # walktree_to_upload("/home/rtheta/parser_pipeline/airflow", pr)
#     # sync_folders()
#
#     worker_task(0, 3)
#     worker_task(1, 3)
#     worker_task(2, 3)
#     # pass
