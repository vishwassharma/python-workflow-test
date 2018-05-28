# import json
# import shutil
# from collections import namedtuple
# from ansible.parsing.dataloader import DataLoader
# from ansible.vars.manager import VariableManager
# from ansible.inventory.manager import InventoryManager
# from ansible.playbook.play import Play
# from ansible.executor.task_queue_manager import TaskQueueManager
# from ansible.plugins.callback import CallbackBase
# import ansible.constants as C
#
#
# class ResultCallback(CallbackBase):
#     """A sample callback plugin used for performing an action as results come in
#
#     If you want to collect all results into a single object for processing at
#     the end of the execution, look into utilizing the ``json`` callback plugin
#     or writing your own custom callback plugin
#     """
#
#     def v2_runner_on_ok(self, result, **kwargs):
#         """Print a json representation of the result
#
#         This method could store the result in an instance attribute for retrieval later
#         """
#         host = result._host
#         print(json.dumps({host.name: result._result}, indent=4))
#
#
# # since API is constructed for CLI it expects certain options
# # to always be set, named tuple 'fakes' the args parsing options object
# Options = namedtuple('Options',
#                      ['connection', 'module_path', 'forks', 'become', 'become_method', 'become_user', 'check', 'diff'])
# options = Options(connection='local', module_path=['/to/mymodules'], forks=10, become=None, become_method=None,
#                   become_user=None, check=False, diff=False)
#
# # initialize needed objects
# loader = DataLoader()  # Takes care of finding and reading yaml, json and ini files
# passwords = dict(vault_pass='secret')
#
# # Instantiate our ResultCallback for handling results as
# # they come in. Ansible expects this to be one of its main display outlets
# results_callback = ResultCallback()
#
# # create inventory, use path to host config file as source or hosts in a comma separated string
# inventory = InventoryManager(loader=loader, sources='localhost,')
#
# # variable manager takes care of merging all the different
# #  sources to give you a unifed view of variables available in each context
# variable_manager = VariableManager(loader=loader, inventory=inventory)
#
# # create datastructure that represents our play, including
# # tasks, this is basically what our YAML loader does internally.
# # play_source = dict(
# #     name="Ansible Play",
# #     hosts='localhost',
# #     gather_facts='no',
# #     tasks=[
# #         dict(action=dict(module='shell', args='ls'), register='shell_out'),
# #         dict(action=dict(module='debug', args=dict(msg='{{shell_out.stdout}}')))
# #     ]
# # )
#
# play_source = dict(
#     name="Compute Engine Instance ansible",
#     hosts='localhost',
#     gather_facts='no',
#     vars=dict(
#         service_account_email="ansible@rtheta-central.iam.gserviceaccount.com",
#         credentials_file="auth.ansible.json",
#         project_id="rtheta-central"
#     ),
#     tasks=[
#         # dict(action=dict(module='shell', args='ls'), register='shell_out'),
#         # dict(action=dict(module='debug', args=dict(msg='{{shell_out.stdout}}')))
#         dict(
#             name="create instances",
#             action=dict(
#                 # module='gce',
#                 module=dict(
#                     name='gce',
#                     instance_names='test1',
#                     zone='us-central1-a',
#                     machine_type='n1-standard-1',
#                     image='debian-8',
#                     state='present',
#                     service_account_email="{{ service_account_email }}",
#                     cresentials_file='{{ credentials_file }}',
#                     project_id='{{ project_id }}',
#                     metadata='{ "startup-script" : "apt-get update" }'
#                 )
#             ),
#             register='gce'
#         )
#     ]
# )
#
# # Create play object, playbook objects use .load instead of init or new methods,
# # this will also automatically create the task objects from the info provided in play_source
# play = Play().load(play_source, variable_manager=variable_manager, loader=loader)
#
# # Run it - instantiate task queue manager, which takes care
# # of forking and setting up all objects to iterate over host list and tasks
# tqm = None
# try:
#     tqm = TaskQueueManager(
#         inventory=inventory,
#         variable_manager=variable_manager,
#         loader=loader,
#         options=options,
#         passwords=passwords,
#         stdout_callback=results_callback,
#         # Use our custom callback instead of the ``default`` callback plugin, which prints to stdout
#     )
#     result = tqm.run(play)  # most interesting data for a play is actually sent to the callback's methods
# except Exception as e:
#     print(e)
# finally:
#     # we always need to cleanup child procs and the structres we use to communicate with them
#     if tqm is not None:
#         tqm.cleanup()
#
#         # Remove ansible tmpdir
#     shutil.rmtree(C.DEFAULT_LOCAL_TMP, True)




# from google.cloud import storage
#
#
# def explicit():
#     from google.cloud import storage
#
#     # Explicitly use service account credentials by specifying the private key
#     # file.
#     storage_client = storage.Client.from_service_account_json(
#         'service_account.json')
#
#     # Make an authenticated API request
#     buckets = list(storage_client.list_buckets())
#     return buckets
#
#
from googleapiclient import discovery
import argparse
import os
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/vishwas/PycharmProjects/rtheta_learning/auth.ansible.json'


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
        project='debian-cloud', family='debian-8').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'gce_conf_script.sh'), 'r').read()
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

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


def main(project, bucket, zone, instance_name, wait=True):
    compute = discovery.build('compute', 'v1')

    print('Creating instance.')

    operation = create_instance(compute, project, zone, instance_name, bucket)
    wait_for_operation(compute, project, zone, operation['name'])

    instances = list_instances(compute, project, zone)

    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])

    print("""
Instance created.
It will take a minute or two for the instance to complete work.
Check this URL: http://storage.googleapis.com/{}/output.png
Once the image is uploaded press enter to delete the instance.
""".format(bucket))

    if wait:
        input("Press any key to delete the instance...")

    print('Deleting instance.')

    operation = delete_instance(compute, project, zone, instance_name)
    wait_for_operation(compute, project, zone, operation['name'])


if __name__ == '__main__':
    main('rtheta-central', 'central.rtheta.in', 'us-central1-a', 'test1')



