import os
import shutil
from tempfile import TemporaryDirectory

from helpers.files import filter_opened_files
from helpers.files import list_directories
from helpers.s3 import get_s3_file_prefix, sync_bucket_folder_and_delete_files
from logger import logging

S3_SYNC_BUCKETS_FOLDER = os.environ.get('S3_SYNC_BUCKETS_FOLDER', os.path.expandvars("$HOME/s3_buckets/sync"))


def main():
    logging.info("Start s3 sync")
    os.makedirs(S3_SYNC_BUCKETS_FOLDER, exist_ok=True)
    s3_buckets = list_directories(S3_SYNC_BUCKETS_FOLDER)
    move_ready_files_to_temp_dir_and_sync(s3_buckets)
    logging.info("Done")


def move_ready_files_to_temp_dir_and_sync(s3_buckets):
    with TemporaryDirectory() as temporary_directory:
        buckets_to_sync = []

        # this step moves files to temporary directory
        for bucket in s3_buckets:
            bucket_directory = os.path.join(S3_SYNC_BUCKETS_FOLDER, bucket)
            for directory, subdirectories, files in os.walk(bucket_directory):
                if files:
                    logging.info(directory)
                    logging.info(files)
                    move_ready_files_to_temp_dir(bucket, directory, files, temporary_directory)
                    buckets_to_sync.append(bucket)

        # this step syncs buckets to s3. aws cli is used for convenience
        for bucket in buckets_to_sync:
            sync_bucket_folder_and_delete_files(bucket, temporary_directory)


def move_ready_files_to_temp_dir(bucket, directory, files, temporary_directory):
    full_path_files = (os.path.join(directory, file)
                       for file in files)
    files_ready_to_sync = filter_opened_files(full_path_files, S3_SYNC_BUCKETS_FOLDER, warnings=False)
    s3_file_prefix = get_s3_file_prefix(bucket, directory, S3_SYNC_BUCKETS_FOLDER)
    destination_folder = os.path.join(temporary_directory, bucket, s3_file_prefix)
    os.makedirs(destination_folder, exist_ok=True)
    for file in files_ready_to_sync:
        filename = os.path.basename(file)
        destination_file = os.path.join(destination_folder, filename)
        shutil.move(file, destination_file)


if __name__ == '__main__':
    main()
