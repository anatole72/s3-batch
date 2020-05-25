import gzip
import hashlib
import json
import os
import re
import subprocess
import tarfile
from datetime import datetime
from tempfile import TemporaryDirectory
from uuid import uuid4
import psutil

import boto3
from elasticsearch.helpers import bulk as elasticsearch_bulk
from elasticsearch.exceptions import ElasticsearchException

from helpers import elasticsearch
from logger import logging

s3_client = boto3.client("s3")

ELASTICSEARCH_LOGS_HOST = os.environ['ELASTICSEARCH_LOGS_HOST']
ELASTICSEARCH_LOGS_PORT = os.environ['ELASTICSEARCH_LOGS_PORT']
MANIFEST_FALLBACK_BUCKET = os.environ['MANIFEST_FALLBACK_BUCKET']
MAX_BATCH_SIZE_MB = int(os.environ.get('MAX_BATCH_SIZE_MB', 1024))
S3_BUCKETS_FOLDER = os.environ.get('S3_BUCKETS_FOLDER', os.path.expandvars("$HOME/buckets"))


def hash_sha256(string):
    try:
        return hashlib.sha256(string.encode()).hexdigest()
    except Exception:
        logging.error("Hash sha256 failed")
        raise


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
    logging.info("Start")
    os.makedirs(S3_BUCKETS_FOLDER, exist_ok=True)
    s3_buckets = list_directories(S3_BUCKETS_FOLDER)

    elasticsearch_docs = create_batch_archives_and_send_to_s3(s3_buckets)

    try:
        send_manifests_to_elasticsearch(elasticsearch_docs)
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
        # the archive creation and s3 sync decouple is done so to avoid race conditions to S3_BUCKETS_FOLDER
        for bucket in s3_buckets:
            bucket_directory = os.path.join(S3_BUCKETS_FOLDER, bucket)
            for directory, subdirectories, files in os.walk(bucket_directory):
                if files:
                    logging.info(directory)
                    logging.info(files)
                    bucket_elastcisearch_docs = create_tar_archives(bucket, directory, files, temporary_directory)
                    if bucket_elastcisearch_docs:
                        elasticsearch_docs += bucket_elastcisearch_docs
                        buckets_to_sync.append(bucket)

        # this step syncs buckets to s3. aws cli is used for convenience
        for bucket in buckets_to_sync:
            directory_to_sync = os.path.join(temporary_directory, bucket)
            subprocess.run(f"aws s3 sync {directory_to_sync} s3://{bucket}", shell=True)
    return elasticsearch_docs


def send_manifests_to_elasticsearch(elasticsearch_docs):
    es = elasticsearch.create_connection(host=ELASTICSEARCH_LOGS_HOST,
                                         port=ELASTICSEARCH_LOGS_PORT,
                                         http_auth=None)
    _, failed = elasticsearch_bulk(es, elasticsearch_docs, stats_only=True)
    if failed:
        raise ElasticsearchException(f"{failed} out of {len(elasticsearch_docs)} failed")


def create_tar_archives(bucket, directory, files, temporary_directory):
    logging.info(bucket)
    logging.info(directory)
    remaining_files_to_archive = files
    s3_file_prefix = get_bucket_directory_and_prefix(bucket, directory)

    elasticsearch_docs = []
    while remaining_files_to_archive:
        archive_files = get_batch_files(directory, remaining_files_to_archive)
        batch_id = uuid4()

        s3_object_key = create_s3_object_key(s3_file_prefix, batch_id)
        remaining_files_to_archive = remaining_files_to_archive[len(archive_files):]
        archive_files = filter_opened_files(archive_files)

        if archive_files:
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


def get_bucket_directory_and_prefix(bucket, directory):
    bucket_directory = os.path.join(S3_BUCKETS_FOLDER, bucket)
    has_prefix = re.match(f"{bucket_directory}/(.*)", directory)
    s3_file_prefix = (has_prefix.group(1)
                      if has_prefix
                      else "")
    return s3_file_prefix


def create_s3_object_key(s3_object_prefix, batch_id):
    tar_basename = f"{batch_id}.tar"
    s3_object_key = f"{s3_object_prefix}/{tar_basename}"
    return s3_object_key


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


s3_opened_files = None


def filter_opened_files(files):
    global s3_opened_files
    if s3_opened_files is None:
        s3_opened_files = [opened_file
                           for opened_files_by_process in get_opened_files()
                           for opened_file in opened_files_by_process
                           if S3_BUCKETS_FOLDER in opened_file]
    return [file for file in files
            if file not in s3_opened_files]


def get_opened_files():
    for pid in psutil.pids():
        try:
            yield (file[0] for file in psutil.Process(pid).open_files())
        except psutil.AccessDenied as e:
            logging.warning("Access denied while getting process opened files")
            logging.warning(str(e))


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
