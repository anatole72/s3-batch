import os

from helpers.files import list_directories
from logger import logging

S3_SYNC_BUCKETS_FOLDER = os.environ.get('S3_SYNC_BUCKETS_FOLDER', os.path.expandvars("$HOME/sync_buckets"))


def main():
    logging.info("Start buckets sync")
    os.makedirs(S3_SYNC_BUCKETS_FOLDER, exist_ok=True)
    s3_buckets = list_directories(S3_SYNC_BUCKETS_FOLDER)


if __name__ == '__main__':
    main()
