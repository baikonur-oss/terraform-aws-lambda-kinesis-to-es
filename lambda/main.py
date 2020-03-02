import datetime
import json
import logging
import os

import boto3
import certifi
from amazon_kinesis_utils import baikonur_logging
from amazon_kinesis_utils import kinesis, misc, s3
from aws_xray_sdk.core import patch
from aws_xray_sdk.core import xray_recorder
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch.helpers import BulkIndexError
from requests_aws4auth import AWS4Auth

# set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("Loading function")

# patch boto3 with X-Ray
libraries = ("boto3", "botocore")
patch(libraries)

# global S3 client instance
s3_client = boto3.client("s3")

# consts
ES_TIMEOUT = 30
BULK_CHUNK_SIZE = 500

# configure with env vars
FAILED_LOG_S3_BUCKET = os.environ["FAILED_LOG_S3_BUCKET"]
FAILED_LOG_S3_PATH_PREFIX = os.environ["FAILED_LOG_S3_PREFIX"]

LOG_ID_FIELD: str = os.environ["LOG_ID_FIELD"]
LOG_TYPE_FIELD: str = os.environ["LOG_TYPE_FIELD"]
LOG_TIMESTAMP_FIELD: str = os.environ["LOG_TIMESTAMP_FIELD"]
LOG_TYPE_UNKNOWN_PREFIX: str = os.environ["LOG_TYPE_UNKNOWN_PREFIX"]

LOG_TYPE_FIELD_WHITELIST_TMP: list = str(os.environ["LOG_TYPE_WHITELIST"]).split(",")
if len(LOG_TYPE_FIELD_WHITELIST_TMP) == 0:
    LOG_TYPE_FIELD_WHITELIST = set()
else:
    LOG_TYPE_FIELD_WHITELIST = set(LOG_TYPE_FIELD_WHITELIST_TMP)

ELASTICSEARCH_HOST = os.environ["ES_HOST"]
INDEX_NAME_PREFIX = os.environ["INDEX_NAME_PREFIX"]

# global auth instance
# do auth because we are outside VPC
aws_auth = AWS4Auth(
    os.environ["AWS_ACCESS_KEY_ID"],
    os.environ["AWS_SECRET_ACCESS_KEY"],
    os.environ["AWS_REGION"],
    "es",
    session_token=os.environ["AWS_SESSION_TOKEN"],
)


def to_str(a):
    if type(a) == dict:
        return json.dumps(a)
    return a


# noinspection PyUnusedLocal
def handler(event, context):
    raw_records = event["Records"]
    logger.debug(raw_records)

    log_dict = dict()
    failed_dict = dict()

    actions = []
    es = Elasticsearch(
        hosts=[{"host": ELASTICSEARCH_HOST, "port": 443}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=ES_TIMEOUT,
        ca_certs=certifi.where(),
    )

    logger.info(f"Connected to Elasticsearch at https://{ELASTICSEARCH_HOST}")

    failed_data_type = []
    failed_data_timestamp = []

    xray_recorder.begin_subsegment("parse")
    for payload in kinesis.parse_records(raw_records):
        try:
            payload_parsed = json.loads(payload)
        except json.JSONDecodeError:
            logger.debug(f"Ignoring non-JSON data: {payload}")
            continue

        baikonur_logging.parse_payload_to_log_dict(
            payload_parsed,
            log_dict,
            failed_dict,
            LOG_TYPE_FIELD,
            LOG_TIMESTAMP_FIELD,
            LOG_ID_FIELD,
            LOG_TYPE_UNKNOWN_PREFIX,
            LOG_TYPE_FIELD_WHITELIST,
            timestamp_required=True,
        )
    xray_recorder.end_subsegment()

    for log_type, v in log_dict:
        records = v["records"]
        for record in records:
            timestamp = record[LOG_TIMESTAMP_FIELD]
            date = datetime.datetime.strftime(timestamp, "%Y%m%d")
            index = f"{INDEX_NAME_PREFIX}-{log_type}-{date}"

            actions.append({"_index": index, "_type": "_doc", "_source": record})

    baikonur_logging.save_json_logs_to_s3(
        s3_client, failed_dict, "failed validation: missing necessary fields, A"
    )

    subsegment = xray_recorder.begin_subsegment("Elasticsearch push")
    subsegment.put_annotation("total_actions", len(actions))

    # good logs save
    failed_data_es = []
    if len(actions) > 0:
        logger.info(
            f"Pushing {len(actions)} actions generated from Kinesis records to Elasticsearch Bulk API"
        )

        for i, chunk in enumerate(misc.split_list(actions, BULK_CHUNK_SIZE)):

            chunk_subsegment = xray_recorder.begin_subsegment(
                "Elasticsearch push chunk"
            )
            chunk_subsegment.put_annotation(
                "chunk_number", i
            )
            chunk_subsegment.put_annotation("chunk_size", len(chunk))
            logger.info(
                f"Sending chunk no. {i} of {len(chunk)} actions"
            )

            try:
                # make sure there will be only one internal chunk/batch
                helpers.bulk(es, chunk, chunk_size=len(chunk))

            except BulkIndexError as e:
                logger.info(
                    f"Got {len(e.errors)} failed actions from Elasticsearch Bulk API"
                )
                failed_data_es += e.errors

            xray_recorder.end_subsegment()

    else:
        logger.info("Nothing to flush")
    xray_recorder.end_subsegment()

    baikonur_logging.save_json_logs_to_s3(s3_client, failed_dict, "One or more necessary fields are unavailable")

    timestamp = datetime.datetime.now()
    key = (
            FAILED_LOG_S3_PATH_PREFIX
            + "/"
            + timestamp.strftime("%Y-%m/%d/%Y-%m-%d-%H:%M:%S")
            + ".gz"
    )
    data = "\n".join(to_str(f) for f in failed_data_es)
    logger.info(f"Saving records rejected by Elasticsearch to S3: s3://{FAILED_LOG_S3_BUCKET}/{key}")
    s3.put_str_data(s3_client, FAILED_LOG_S3_BUCKET, key, data, gzip_compress=True)

    logger.info(f"Finished")
