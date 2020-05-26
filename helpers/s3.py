import os
import re
from uuid import uuid4

from s3_batch import S3_BATCH_BUCKETS_FOLDER


def get_s3_file_prefix(bucket, directory):
    bucket_directory = os.path.join(S3_BATCH_BUCKETS_FOLDER, bucket)
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