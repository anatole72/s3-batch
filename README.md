
<br />  
<p align="center">
    <img src="icons/s3_batch.png" alt="Logo" width="120" height="120">
<h3 align="center">S3 Batch</h3>
<p align="center">  
    Archives groups of files and sends to S3, indexing in ElasticSearch
</p>
<p align="center">
    <a href="https://sonarcloud.io/dashboard?id=lettdigital_s3-batch" alt="Quality">
        <img src="https://sonarcloud.io/api/project_badges/measure?project=lettdigital_s3-batch&metric=alert_status"/></a>
            <a href="https://sonarcloud.io/dashboard?id=lettdigital_s3-batch" alt="Security">
        <img src="https://sonarcloud.io/api/project_badges/measure?project=lettdigital_s3-batch&metric=security_rating"/></a>
            <a href="https://sonarcloud.io/dashboard?id=lettdigital_s3-batch" alt="Bugs">
        <img src="https://sonarcloud.io/api/project_badges/measure?project=lettdigital_s3-batch&metric=bugs"/></a>
 </p>

# Table of Contents

  * [Installation and Execution](#installation-and-execution)
  * [Configuration](#configuration)
  * [Documentation](#documentation)
    + [How does the batch work?](#how-does-the-batch-work-)
  * [Files ingestion](#files-ingestion)
- [References](#references)

# Installation and Execution

- `pipenv install`

- `pipenv run python s3_batch.py`

- `pipenv run python s3_sync.py`

If you do not want to rely on pipenv, you can always use pip to install it locally:

```bash
pipenv run pip lock > requirements.txt
pip install -r requirements.txt -t $(pwd)
```

# Configuration

The configuration is done through environment variables:

| Environment Variable | Description  | Required | Default |
|--|--|--|--|
| ELASTICSEARCH_LOGS_HOST | ElasticSearch Host for sending batch manifests | yes | - |
| ELASTICSEARCH_LOGS_PORT | ElasticSearch host port for communication| yes | - |
| RSYSLOG_HOST | Optional rsyslog remote server for logs ingestion | no | None |
| RSYSLOG_PORT | rsyslog remote server port | no | None |
| MAX_BATCH_SIZE_MB | Configures maximum batch size (in megabytes) | no | 1024 |
| MANIFEST_FALLBACK_BUCKET | Fallback bucket in case ElasticSearch is unavailable | yes | - |
| S3_BATCH_BUCKETS_FOLDER | Local folder where the script will look for files to create batch archives | no | "$HOME/s3_buckets/batch" |
| S3_SYNC_BUCKETS_FOLDER | Local folder where the script will look for files to sync to S3 | no | "$HOME/s3_buckets/sync" |

# Documentation

The S3 Batch works by looking for files in a specific directory and uploading the created archives to S3. It respects the original intended partition of the object key.

The script expects the directory path to use the following structure:

```
$S3_BATCH_BUCKETS_FOLDER/<bucket-name>/<object-key>
```

Therefore, if a file is placed under the path `$S3_BATCH_BUCKETS_FOLDER/example-bucket/example/path/to/object/file.txt`, the script will create an archive that reflects the original object key partition under the new object key `example/path/to/object/<uuid>.tar` in the bucket `example-bucket`. `example/path/to/object/<uuid>.tar` will contain all the files originally present in the directory.

S3 Sync uses the same structure, but instead of creating an archive it just uploads the original files to S3. It is useful for applications that do not directly interacts with S3:

```
$S3_SYNC_BUCKETS_FOLDER/<bucket-name>/<object-key>
```

## How does the batch work?

- It begins by listing all the files in the directory and locking the list to that specific moment.
- Then it lists all the currently opened files using `psutil`.
- All files opened by other processes are filtered out of the locked files list.
- It creates archives up to the desired maximum size in a new temporary location using python's `tempfile`. It deletes the original files immediately after creating the archive. Therefore it is required for the machine running the script to have at least `$MAX_BATCH_SIZE_MB` free space in `/tmp`.
- All archiving operations are done before beginning the upload operation. This is done to minimize the chance of collision between runs.
- After uploading the archive files in the temporary directory to the S3 bucket, the script sends to ElasticSearch entries tying the original object keys to the new archive key in S3, with the correct URL location for easy access from Kibana.
- In case of communication failure with ElasticSearch, the archives manifest is sent to S3 in JSON format.

# Files ingestion

There are many ways for sharing files between computers. The most common is SFTP, which uses SSH as the underlying authentication method. Use this method if you want to publicly expose this application.

Beware of the fact that SFTP is very CPU intensive. I was able to ingest around 5 files simultaneously in a `t3.nano` in AWS.

For a more performant solution, consider using FTPS (FTP over TLS). It is able to ingest around 170 files simultaneously using the same EC2 Type. [1]

# References

[1] https://www.digitalocean.com/community/tutorials/how-to-configure-vsftpd-to-use-ssl-tls-on-a-centos-vps
