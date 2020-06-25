import os
import re
import subprocess
from uuid import uuid4
import boto3


s3_client = boto3.client("s3")


def get_s3_file_prefix(bucket, directory, buckets_folder):
    bucket_directory = os.path.join(buckets_folder, bucket)
    has_prefix = re.match(f"{bucket_directory}/(.*)", directory)
    s3_file_prefix = (has_prefix.group(1)
                      if has_prefix
                      else "")
    return s3_file_prefix


def create_batch_s3_key(s3_object_prefix):
    batch_id = uuid4()
    tar_basename = f"{batch_id}.tar"
    s3_object_key = f"{s3_object_prefix}/{tar_basename}"
    return s3_object_key


def sync_bucket_folder_and_delete_files(bucket, buckets_directory):
    directory_to_sync = os.path.join(buckets_directory, bucket)
    for dir, subdirs, files in os.walk(directory_to_sync):
        for file in files:
            filename = os.path.join(dir, file)
            key = os.path.relpath(filename,
                                  directory_to_sync)
            s3_client.upload_file(filename, bucket, key)
            os.remove(filename)
