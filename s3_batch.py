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
                create_batches_and_upload(bucket, directory, files, es)


def create_batches_and_upload(bucket, directory, files, elasticsearch_connection):
    logging.info(bucket)
    logging.info(directory)
    remaining_files_to_compact = files
    s3_file_prefix = get_bucket_directory_and_prefix(bucket, directory)

    elasticsearch_docs = []
    with TemporaryDirectory() as temporary_directory:
        while remaining_files_to_compact:
            tar = tarfile.open(os.path.join(temporary_directory, f"{uuid4()}.tar"), "w:")

            archive_files = get_batch_files(directory, remaining_files_to_compact)
            write_and_close_archive(archive_files, directory, tar)
            remaining_files_to_compact = remaining_files_to_compact[len(archive_files):]
            s3_object_key, s3_object_prefix = send_archive_to_s3(bucket, s3_file_prefix, tar)

            elasticsearch_doc = get_batch_elasticsearch_doc(archive_files,
                                                            bucket,
                                                            s3_object_key,
                                                            s3_object_prefix,
                                                            tar)
            elasticsearch_docs.append(elasticsearch_doc)

            logging.info(archive_files)
            logging.info(os.path.join(bucket, s3_object_key))

    write_batch_indexes_to_elasticsearch(elasticsearch_connection, elasticsearch_docs)


def get_batch_elasticsearch_doc(archive_files, bucket, s3_object_key, s3_object_prefix, tar):
    return {"_id": os.path.join(bucket, s3_object_key),
            "bucket": bucket,
            "name": os.path.splitext(os.path.basename(tar.name))[0],
            "ext": os.path.splitext(os.path.basename(tar.name))[1],
            "prefix": s3_object_prefix,
            "archive_content": archive_files}


def get_bucket_directory_and_prefix(bucket, directory):
    bucket_directory = os.path.join(S3_BUCKETS_FOLDER, bucket)
    has_prefix = re.match(f"{bucket_directory}/(.*)", directory)
    s3_file_prefix = (has_prefix.group(1)
                      if has_prefix
                      else "")
    return s3_file_prefix


def write_batch_indexes_to_elasticsearch(elasticsearch_connection, elasticsearch_docs):
    date = datetime.utcnow().strftime("%Y.%m.%d")
    elasticsearch_bulk_files = [{"_index": f"s3-bulk-{date}",
                                 "_type": "_doc",
                                 **doc}
                                for doc in elasticsearch_docs]
    elasticsearch_bulk(elasticsearch_connection,
                       elasticsearch_bulk_files)


def send_archive_to_s3(bucket, s3_file_prefix, tar):
    creation_date = datetime.fromtimestamp(os.path.getmtime(tar.name))
    tar_basename = os.path.basename(tar.name)
    tar_date_partition = f"year={creation_date.year}/month={creation_date.month}/day={creation_date.day}"
    s3_object_prefix = f"{s3_file_prefix}/{tar_date_partition}"
    s3_object_key = f"{s3_object_prefix}/{tar_basename}"
    s3_client.upload_file(tar.name, bucket, s3_object_key)
    return s3_object_key, s3_object_prefix


def write_and_close_archive(archive_files, directory, tar):
    for archive_file in archive_files:
        tar.add(os.path.join(directory, archive_file),
                arcname=archive_file)
    tar.close()


def get_batch_files(directory, remaining_files_to_compact):
    batch_size = 0
    archive_files = [
        file for file in remaining_files_to_compact
        if (batch_size := batch_size + get_file_size_mb(os.path.join(directory, file))) <= MAX_BATCH_SIZE_MB
    ]
    if not archive_files and remaining_files_to_compact:
        archive_files = remaining_files_to_compact[:1]
    return archive_files


if __name__ == '__main__':
    main()
