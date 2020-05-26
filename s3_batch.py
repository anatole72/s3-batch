import gzip
import json
import os
import tarfile
from datetime import datetime
from tempfile import TemporaryDirectory

import boto3

from helpers.elasticsearch import hash_sha256, send_docs_to_elasticsearch
from helpers.files import (filter_opened_files,
                           get_basename_without_extension,
                           get_file_size_mb,
                           get_filename_extension,
                           list_directories)
from helpers.s3 import create_batch_s3_key, get_s3_file_prefix, sync_bucket_folder
from logger import logging

s3_client = boto3.client("s3")

ELASTICSEARCH_LOGS_HOST = os.environ['ELASTICSEARCH_LOGS_HOST']
ELASTICSEARCH_LOGS_PORT = os.environ['ELASTICSEARCH_LOGS_PORT']
MANIFEST_FALLBACK_BUCKET = os.environ['MANIFEST_FALLBACK_BUCKET']
MAX_BATCH_SIZE_MB = int(os.environ.get('MAX_BATCH_SIZE_MB', 1024))
S3_BATCH_BUCKETS_FOLDER = os.environ.get('S3_BATCH_BUCKETS_FOLDER', os.path.expandvars("$HOME/batch_buckets"))


def main():
    logging.info("Start s3 batch")
    os.makedirs(S3_BATCH_BUCKETS_FOLDER, exist_ok=True)
    s3_buckets = list_directories(S3_BATCH_BUCKETS_FOLDER)

    elasticsearch_docs = create_batch_archives_and_send_to_s3(s3_buckets)

    try:
        send_docs_to_elasticsearch(elasticsearch_docs)
    except Exception as error:
        log_error_and_upload_manifests_to_s3(error, elasticsearch_docs)

    logging.info("Done")


def log_error_and_upload_manifests_to_s3(error, elasticsearch_docs):
    logging.error("Exception caught while sending manifests to elasticsearch")
    logging.error(str(error))
    logging.info("Uploading manifests to s3 fallback bucket")
    s3_client.put_object(Bucket=MANIFEST_FALLBACK_BUCKET,
                         Key=os.path.join(f"s3-batch/manifests/{datetime.utcnow().strftime('%Y-%m-%d')}.json.gz"),
                         Body=gzip.compress(json.dumps(elasticsearch_docs).encode("utf-8")),
                         ACL="private")


def create_batch_archives_and_send_to_s3(s3_buckets):
    elasticsearch_docs = []
    with TemporaryDirectory() as temporary_directory:
        buckets_to_sync = []

        # this step only creates the archives and deletes original files
        # the archive creation and s3 sync decouple is done so to avoid race conditions to S3_SYNC_BUCKETS_FOLDER
        for bucket in s3_buckets:
            bucket_directory = os.path.join(S3_BATCH_BUCKETS_FOLDER, bucket)
            for directory, subdirectories, files in os.walk(bucket_directory):
                if files:
                    logging.info(directory)
                    logging.info(files)
                    bucket_elasticsearch_docs = create_tar_archives(bucket, directory, files, temporary_directory)
                    if bucket_elasticsearch_docs:
                        elasticsearch_docs += bucket_elasticsearch_docs
                        buckets_to_sync.append(bucket)

        # this step syncs buckets to s3. aws cli is used for convenience
        for bucket in buckets_to_sync:
            sync_bucket_folder(bucket, temporary_directory)
    return elasticsearch_docs


def create_tar_archives(bucket, directory, files, temporary_directory):
    logging.info(bucket)
    logging.info(directory)
    remaining_files_to_archive = files
    s3_file_prefix = get_s3_file_prefix(bucket, directory, S3_BATCH_BUCKETS_FOLDER)

    elasticsearch_docs = []
    while remaining_files_to_archive:
        archive_files = get_batch_files(directory, remaining_files_to_archive)
        remaining_files_to_archive = remaining_files_to_archive[len(archive_files):]
        archive_files = filter_opened_files(archive_files, S3_BATCH_BUCKETS_FOLDER)

        if archive_files:
            s3_object_key = create_batch_s3_key(s3_file_prefix)
            tar_info = create_archive(bucket, s3_object_key, archive_files, temporary_directory)
            elasticsearch_docs += get_batch_elasticsearch_docs(bucket, s3_object_key, tar_info)

            for archive_file in archive_files:
                os.remove(archive_file)

    return elasticsearch_docs


def get_batch_elasticsearch_docs(bucket, s3_object_key, tar_info):
    object_url = os.path.join("https://s3.console.aws.amazon.com/s3/object", bucket, s3_object_key)
    date = tar_info["modification_date"].strftime("%Y.%m.%d")
    return [{"_id": hash_sha256(f"{os.path.join(bucket, s3_object_key)}:{member_name}"),
             "_index": f"s3-batch-{date}",
             "_type": "_doc",
             "bucket": bucket,
             "url": object_url,
             "name": get_basename_without_extension(tar_info["name"]),
             "archive_content": member_name,
             "archive_content_name": get_basename_without_extension(member_name),
             "archive_content_extension": get_filename_extension(member_name)}
            for member_name in tar_info["members_names"]]


def create_archive(bucket, s3_object_key, archive_files, archive_dir):
    archive_full_path = os.path.join(archive_dir, bucket, s3_object_key)
    os.makedirs(os.path.dirname(archive_full_path), exist_ok=True)
    tar = tarfile.open(archive_full_path, "w:")
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
