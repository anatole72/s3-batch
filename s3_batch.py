import os

import boto3

from helpers import elasticsearch

s3_client = boto3.client("s3")

ELASTICSEARCH_LOGS_HOST = os.environ['ELASTICSEARCH_LOGS_HOST']
ELASTICSEARCH_LOGS_PORT = os.environ['ELASTICSEARCH_LOGS_PORT']
MAX_BATCH_SIZE_MB = os.environ.get('MAX_BATCH_SIZE_MB', 1024)
S3_BUCKETS_FOLDER = os.environ.get('S3_BUCKETS_FOLDER', os.path.expandvars("$HOME/buckets"))


def main():
    es = elasticsearch.create_connection(host=ELASTICSEARCH_LOGS_HOST,
                                         port=ELASTICSEARCH_LOGS_PORT,
                                         http_auth=None)

    os.makedirs(S3_BUCKETS_FOLDER, exist_ok=True)
    buckets_folder_tree = os.walk(S3_BUCKETS_FOLDER)
    s3_buckets = next(buckets_folder_tree)[1]
    files_by_bucket = {os.path.basename(buckets_folder_node[0]): buckets_folder_node[2]
                       for buckets_folder_node in buckets_folder_tree}
    print(files_by_bucket)


if __name__ == '__main__':
    main()
