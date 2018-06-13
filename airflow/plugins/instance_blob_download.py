import os
from google.cloud import storage

DESTINATION_BLOB_NAME = 'airflow_home'

airflow_home = os.environ.get('AIRFLOW_HOME')


def make_dirs(path):
    if os.path.exists(path):
        return
    else:
        make_dirs(os.path.dirname(path))
        os.mkdir(path)


def download_blob(bucket_name='central.rtheta.in', source_blob_name=DESTINATION_BLOB_NAME):
    # TODO: remove the hardcoding
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        if blob.name.startswith(source_blob_name):
            rel_file_path = blob.name.replace(source_blob_name + '/', "")
            file_path = os.path.join(airflow_home, rel_file_path)
            make_dirs(os.path.dirname(file_path))  # for creating the path recursively
            blob.download_to_filename(file_path)
            print (file_path)


if __name__ == "__main__":

    # make_dirs(airflow_home)
    download_blob()
    os.chdir(airflow_home)
