import base64
import datetime
import gzip
import io
import json
import logging
import os
import uuid
from json import JSONDecodeError

import boto3
import certifi
import dateutil
from aws_kinesis_agg.deaggregator import iter_deaggregate_records
from aws_xray_sdk.core import patch
from aws_xray_sdk.core import xray_recorder
from boto3.exceptions import S3UploadFailedError
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch.helpers import BulkIndexError
from requests_aws4auth import AWS4Auth

# set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Loading function')

# when debug logging is needed, uncomment following lines:
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

# patch boto3 with X-Ray
libraries = ('boto3', 'botocore')
patch(libraries)

# global S3 client instance
s3 = boto3.client('s3')

# consts
ES_TIMEOUT = 30
BULK_CHUNK_SIZE = 500

# configure with env vars
FAILED_LOG_S3_BUCKET = os.environ['FAILED_LOG_S3_BUCKET']
FAILED_LOG_S3_PATH_PREFIX = os.environ['FAILED_LOG_S3_PREFIX']

LOG_TYPE_FIELD: str = os.environ['LOG_TYPE_FIELD']
LOG_TIMESTAMP_FIELD: str = os.environ['LOG_TIMESTAMP_FIELD']
LOG_TYPE_FIELD_WHITELIST: set = set(str(os.environ['LOG_TYPE_WHITELIST']).split(','))
LOG_TYPE_UNKNOWN_PREFIX: str = os.environ['LOG_TYPE_UNKNOWN_PREFIX']

ELASTICSEARCH_HOST = os.environ['ES_HOST']
INDEX_NAME_PREFIX = os.environ['INDEX_NAME_PREFIX']

# global auth instance
# do auth because we are outside VPC
aws_auth = AWS4Auth(os.environ['AWS_ACCESS_KEY_ID'],
                    os.environ['AWS_SECRET_ACCESS_KEY'],
                    os.environ['AWS_REGION'],
                    'es',
                    session_token=os.environ['AWS_SESSION_TOKEN'])


def put_to_s3(key: str, bucket: str, data: str):
    # gzip and put data to s3 in-memory
    xray_recorder.begin_subsegment('gzip compress')
    data_gz = gzip.compress(data.encode(), compresslevel=9)
    xray_recorder.end_subsegment()

    xray_recorder.begin_subsegment('s3 upload')
    try:
        with io.BytesIO(data_gz) as data_gz_fileobj:
            s3_results = s3.upload_fileobj(data_gz_fileobj, bucket, key)

        logger.info(f"S3 upload errors: {s3_results}")

    except S3UploadFailedError as e:
        logger.error("Upload failed. Error:")
        logger.error(e)
        import traceback
        traceback.print_stack()
        raise
    xray_recorder.end_subsegment()


def normalize_kinesis_payload(payload: dict):
    # Normalize messages from CloudWatch (subscription filters) and pass through anything else
    # https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/logs/SubscriptionFilters.html

    logger.debug(f"normalizer input: {payload}")

    payloads = []

    if len(payload) < 1:
        logger.error(f"Got weird record: \"{payload}\", skipping")
        return payloads

    # check if data is JSON and parse
    try:
        payload = json.loads(payload)

    except JSONDecodeError:
        logger.error(f"Non-JSON data found: {payload}, skipping")
        return payloads

    if 'messageType' in payload:
        logger.debug(f"Got payload looking like CloudWatch Logs via subscription filters: "
                     f"{payload}")

        if payload['messageType'] == "DATA_MESSAGE":
            if 'logEvents' in payload:
                for event in payload['logEvents']:
                    # check if data is JSON and parse
                    try:
                        logger.debug(f"message: {event['message']}")
                        payload_parsed = json.loads(event['message'])
                        logger.debug(f"parsed payload: {payload_parsed}")

                    except JSONDecodeError:
                        logger.debug(f"Non-JSON data found inside CWL message: {event}, giving up")
                        continue

                    payloads.append(payload_parsed)

            else:
                logger.error(f"Got DATA_MESSAGE from CloudWatch but logEvents are not present, "
                             f"skipping payload: {payload}")

        elif payload['messageType'] == "CONTROL_MESSAGE":
            logger.info(f"Got CONTROL_MESSAGE from CloudWatch: {payload}, skipping")
            return payloads

        else:
            logger.error(f"Got unknown messageType, shutting down")
            raise ValueError(f"Unknown messageType: {payload}")
    else:
        payloads.append(payload)
        
    return payloads


def bad_data_save(data: list, reason: str):
    # simply dump list of whatever to string, gzip it and upload to s3
    if len(data) > 0:
        xray_recorder.begin_subsegment(f"bad data upload: {reason}")

        logger.error(f"Got {len(data)} failed Kinesis records ({reason})")
        timestamp = datetime.datetime.now()
        key = FAILED_LOG_S3_PATH_PREFIX + '/' + timestamp.strftime("%Y-%m/%d/%Y-%m-%d-%H:%M:%S-")
        key += str(uuid.uuid4()) + ".gz"

        logger.info(f"Saving failed records to S3: s3://{FAILED_LOG_S3_BUCKET}/{key}")
        data = '\n'.join(str(f) for f in data)
        put_to_s3(key, FAILED_LOG_S3_BUCKET, data)

        xray_recorder.end_subsegment()


# noinspection PyUnusedLocal
def handler(event, context):
    raw_records = event['Records']
    actions = []
    es = Elasticsearch(hosts=[{'host': ELASTICSEARCH_HOST, 'port': 443}],
                       http_auth=aws_auth,
                       use_ssl=True,
                       verify_certs=True,
                       connection_class=RequestsHttpConnection,
                       timeout=ES_TIMEOUT,
                       ca_certs=certifi.where())

    logger.info(f"Connected to Elasticsearch at https://{ELASTICSEARCH_HOST}")

    failed_data_type = []
    failed_data_timestamp = []
    failed_data_es = []

    processed_records = 0

    subsegment = xray_recorder.begin_subsegment('parse_records')
    for record in iter_deaggregate_records(raw_records):
        logger.debug(f"raw Kinesis record: {record}")
        # Kinesis data is base64 encoded
        decoded_data = base64.b64decode(record['kinesis']['data'])

        # check if base64 contents is gzip
        # gzip magic number 0x1f 0x8b
        if decoded_data[0] == 0x1f and decoded_data[1] == 0x8b:
            decoded_data = gzip.decompress(decoded_data)

        decoded_data = decoded_data.decode()
        normalized_payloads = normalize_kinesis_payload(decoded_data)
        logger.debug(f"Normalized payloads: {normalized_payloads}")

        for normalized_payload in normalized_payloads:
            logger.debug(f"Parsing normalized payload: {normalized_payload}")

            processed_records += 1

            # check if log type field is available
            try:
                log_type = normalized_payload[LOG_TYPE_FIELD]

            except KeyError:
                logger.error(f"Cannot retrieve necessary field \"{LOG_TYPE_FIELD}\" "
                             f"from payload: {normalized_payload}")
                logger.error(f"Will save failed record to S3")
                failed_data_type.append(normalized_payload)
                continue

            # apply whitelist in-place

            if len(LOG_TYPE_FIELD_WHITELIST) != 0 and log_type not in LOG_TYPE_FIELD_WHITELIST:
                logger.debug(f"Skipping ignored log_type: {log_type}")
                continue

            # check if timestamp is present
            try:
                timestamp = normalized_payload[LOG_TIMESTAMP_FIELD]
                timestamp = dateutil.parser.parse(timestamp)

            except KeyError:
                logger.error(f"Cannot retrieve necessary field \"{LOG_TIMESTAMP_FIELD}\" "
                             f"from payload: {normalized_payload}")
                logger.error(f"Will save failed record to S3")
                failed_data_timestamp.append(normalized_payload)
                continue

            # valid data
            date = datetime.datetime.strftime(timestamp, "%Y%m%d")
            index = f"{INDEX_NAME_PREFIX}-{log_type}-{date}"

            if len(LOG_TYPE_FIELD_WHITELIST) != 0 and log_type not in LOG_TYPE_FIELD_WHITELIST:
                logger.info(f"Log type {log_type} not in whitelist {LOG_TYPE_FIELD_WHITELIST}")
                continue

            actions.append({"_index":  index,
                            "_type":   "_doc",
                            "_source": normalized_payload})

    logger.info(f"Processed {processed_records} records from Kinesis")
    subsegment.put_annotation("processed_records", processed_records)
    xray_recorder.end_subsegment()

    subsegment = xray_recorder.begin_subsegment('Elasticsearch push')
    subsegment.put_annotation("total_actions", len(actions))
    # good logs save
    if len(actions) > 0:
        logger.info(f"Pushing {len(actions)} actions generated from Kinesis records to Elasticsearch Bulk API")

        for i in range(0, len(actions), BULK_CHUNK_SIZE):
            chunk_subsegment = xray_recorder.begin_subsegment('Elasticsearch push chunk')
            actions_chunk = actions[i:i + BULK_CHUNK_SIZE]

            chunk_subsegment.put_annotation("chunk_number", int(i / BULK_CHUNK_SIZE + 1))
            chunk_subsegment.put_annotation("chunk_size", len(actions_chunk))
            logger.info(f"Sending chunk no. {int(i / BULK_CHUNK_SIZE + 1)} of {len(actions_chunk)} actions")

            try:
                # make sure there will be only one internal chunk/batch
                helpers.bulk(es, actions_chunk,
                             chunk_size=len(actions_chunk))

            except BulkIndexError as e:
                logger.info(f"Got {len(e.errors)} failed actions from Elasticsearch Bulk API")
                failed_data_es += e.errors

            xray_recorder.end_subsegment()

    else:
        logger.info("Nothing to flush")
    xray_recorder.end_subsegment()

    bad_data_save(failed_data_type,
                  reason=f"missing log type field {LOG_TYPE_FIELD}")
    bad_data_save(failed_data_timestamp,
                  reason=f"missing timestamp in field {LOG_TIMESTAMP_FIELD}")
    bad_data_save(failed_data_es,
                  reason="rejected by Elasticsearch")

    logger.info(f"Finished")
