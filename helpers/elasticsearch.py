import hashlib
import logging
import os

from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import bulk

from logger import logging

ELASTICSEARCH_LOGS_HOST = os.environ['ELASTICSEARCH_LOGS_HOST']
ELASTICSEARCH_LOGS_PORT = os.environ['ELASTICSEARCH_LOGS_PORT']


def create_connection(**elastic_credentials_kwargs):
    try:
        elastic_connection = Elasticsearch(
            elastic_credentials_kwargs['host'],
            port=elastic_credentials_kwargs['port'],
            http_auth=(elastic_credentials_kwargs["http_auth"]
                       if "http_auth" in elastic_credentials_kwargs else
                       (elastic_credentials_kwargs['username'], elastic_credentials_kwargs['password'])),
            use_ssl=elastic_credentials_kwargs.get("use_ssl", False),
            verify_certs=elastic_credentials_kwargs.get("verify_certs", False)
        )
        if not elastic_connection.ping():
            raise Exception("Unable to reach elasticsearch REST server")

        logging.debug("Get Elasticsearch connection successful")
        return elastic_connection
    except Exception:
        logging.error("Get Elasticsearch connection failed")
        raise


def send_bulk(elastic_connection, elastic_docs):
    try:
        bulk(elastic_connection, elastic_docs)
        logging.debug("Send documents to Elasticsearch success")
    except Exception:
        logging.error("Send documents to Elasticsearch failed")
        raise


def hash_sha256(string):
    try:
        return hashlib.sha256(string.encode()).hexdigest()
    except Exception:
        logging.error("Hash sha256 failed")
        raise


def send_docs_to_elasticsearch(elasticsearch_docs):
    es = create_connection(host=ELASTICSEARCH_LOGS_HOST,
                           port=ELASTICSEARCH_LOGS_PORT,
                           http_auth=None)
    _, failed = bulk(es, elasticsearch_docs, stats_only=True)
    if failed:
        raise ElasticsearchException(f"{failed} out of {len(elasticsearch_docs)} failed")
