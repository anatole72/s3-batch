import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


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

        logging.info("Get Elasticsearch connection successful")
        return elastic_connection
    except Exception:
        logging.error("Get Elasticsearch connection failed")
        raise


def send_bulk(elastic_connection, elastic_docs):
    try:
        bulk(elastic_connection, elastic_docs)
        logging.info("Send documents to Elasticsearch success")
    except Exception:
        logging.error("Send documents to Elasticsearch failed")
        raise
