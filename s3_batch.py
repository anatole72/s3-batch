import os
import re
import tarfile
from datetime import datetime
from tempfile import TemporaryDirectory
from uuid import uuid4

import boto3
from elasticsearch.helpers import bulk as elasticsearch_bulk

from helpers import elasticsearch
from logger import logging

s3_client = boto3.client("s3")

ELASTICSEARCH_LOGS_HOST = os.environ['ELASTICSEARCH_LOGS_HOST']
ELASTICSEARCH_LOGS_PORT = os.environ['ELASTICSEARCH_LOGS_PORT']
MAX_BATCH_SIZE_MB = int(os.environ.get('MAX_BATCH_SIZE_MB', 1024))
S3_BUCKETS_FOLDER = os.environ.get('S3_BUCKETS_FOLDER', os.path.expandvars("$HOME/buckets"))


def get_file_size_mb(file_path):
    file_stats = os.stat(file_path)
    return file_stats.st_size / (1024 * 1024)


def list_files(path):
    return next(os.walk(path))[2]


def list_directories(path):
    return next(os.walk(path))[1]


def get_basename_without_extension(filename):
    return os.path.splitext(os.path.basename(filename))[0]


def get_filename_extension(filename):
    return os.path.splitext(os.path.basename(filename))[1]


def main():
    es = elasticsearch.create_connection(host=ELASTICSEARCH_LOGS_HOST,
                                         port=ELASTICSEARCH_LOGS_PORT,
                                         http_auth=None)

    os.makedirs(S3_BUCKETS_FOLDER, exist_ok=True)
    s3_buckets = list_directories(S3_BUCKETS_FOLDER)

    for bucket in s3_buckets:
        bucket_directory = os.path.join(S3_BUCKETS_FOLDER, bucket)
        for directory, subdirectories, files in os.walk(bucket_directory):
            if files:
                create_upload_and_index_batches(bucket, directory, files, es)
    logging.info("Done")


def create_upload_and_index_batches(bucket, directory, files, elasticsearch_connection):
    logging.info(bucket)
    logging.info(directory)
    remaining_files_to_archive = files
    s3_file_prefix = get_bucket_directory_and_prefix(bucket, directory)

    elasticsearch_docs = []
    with TemporaryDirectory() as temporary_directory:
        while remaining_files_to_archive:
            archive_files = get_batch_files(directory, remaining_files_to_archive)
            tar_info = create_archive(archive_files, temporary_directory)
            remaining_files_to_archive = remaining_files_to_archive[len(archive_files):]

            s3_object_key = send_archive_to_s3(bucket, s3_file_prefix, tar_info)

            batch_elasticsearch_doc = get_batch_elasticsearch_docs(bucket, s3_object_key, tar_info)
            elasticsearch_docs += batch_elasticsearch_doc

            for archive_file in archive_files:
                os.remove(archive_file)

            logging.info("; ".join(archive_files))
            logging.info(os.path.join(bucket, s3_object_key))

    elasticsearch_bulk(elasticsearch_connection,
                       elasticsearch_docs)


def get_batch_elasticsearch_docs(bucket, s3_object_key, tar_info):
    object_url = os.path.join("https://s3.console.aws.amazon.com/s3/object", bucket, s3_object_key)
    date = tar_info["modification_date"].strftime("%Y.%m.%d")
    return [{"_id": f"{os.path.join(bucket, s3_object_key)}:{member_name}",
             "_index": f"s3-batch-{date}",
             "_type": "_doc",
             "bucket": bucket,
             "url": object_url,
             "name": get_basename_without_extension(tar_info["name"]),
             "archive_content": member_name,
             "archive_content_name": get_basename_without_extension(member_name),
             "archive_content_extension": get_filename_extension(member_name)}
            for member_name in tar_info["members_names"]]


def get_bucket_directory_and_prefix(bucket, directory):
    bucket_directory = os.path.join(S3_BUCKETS_FOLDER, bucket)
    has_prefix = re.match(f"{bucket_directory}/(.*)", directory)
    s3_file_prefix = (has_prefix.group(1)
                      if has_prefix
                      else "")
    return s3_file_prefix


def send_archive_to_s3(bucket, s3_file_prefix, tar_info):
    modification_date = tar_info["modification_date"]
    tar_basename = os.path.basename(tar_info["name"])
    tar_date_partition = f"year={modification_date.year}/month={modification_date.month}/day={modification_date.day}"
    s3_object_prefix = f"{s3_file_prefix}/{tar_date_partition}"
    s3_object_key = f"{s3_object_prefix}/{tar_basename}"
    s3_client.upload_file(tar_info["name"], bucket, s3_object_key)
    return s3_object_key


def create_archive(archive_files, archive_dir):
    tar = tarfile.open(os.path.join(archive_dir, f"{uuid4()}.tar"), "w:")
    for archive_file in archive_files:
        tar.add(archive_file,
                arcname=os.path.basename(archive_file))
    tar_info = {
        "name": tar.name,
        "members_names": [member.name for member in tar.getmembers()],
    }
    tar.close()
    tar_info["modification_date"] = datetime.fromtimestamp(os.path.getmtime(tar_info["name"]))
    return tar_info


def get_batch_files(directory, remaining_files_to_archive):
    batch_size = 0
    archive_files = [
        file for file in remaining_files_to_archive
        if (batch_size := batch_size + get_file_size_mb(os.path.join(directory, file))) <= MAX_BATCH_SIZE_MB
    ]
    if not archive_files and remaining_files_to_archive:
        archive_files = remaining_files_to_archive[:1]
    return [os.path.join(directory, archive_file)
            for archive_file in archive_files]


if __name__ == '__main__':
    main()
