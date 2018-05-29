import os
from google.cloud import storage


def make_dirs(path):
    if os.path.exists(path):
        return
    else:
        make_dirs(os.path.dirname(path))
        os.mkdir(path)


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

if __name__ == "__main__":
    download_blob()
