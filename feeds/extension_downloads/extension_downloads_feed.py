import boto3
from botocore.exceptions import ClientError
from collections import OrderedDict
import json
import re

from utils.ducklake import DuckLakeConnection

EXTENSION_DOWNLOADS_TABLE = 'extension_downloads'

S3_BUCKET = 'duckdb-extensions'
S3_BUCKET_DIR = 'download-stats-weekly'


def run():
    print("------------------")
    print("running extension_dowloads_feed ...")

    # fetch periods already stored in ducklake
    with DuckLakeConnection() as con:
        if con.table_exists(EXTENSION_DOWNLOADS_TABLE):
            periods_in_ducklake = con.sql(f"select distinct year, week from {EXTENSION_DOWNLOADS_TABLE}").fetchall()
        else:
            periods_in_ducklake = []

    # fetch download stats for new periods from s3
    new_records = []
    s3_client = boto3.client('s3')
    s3_file_paths = get_s3_file_paths(s3_client)
    for file_path in s3_file_paths:
        iso_year_str, _, iso_week_str = (
            file_path.removeprefix('download-stats-weekly/').removesuffix('.json').partition('/')
        )
        if not is_valid_iso_year(iso_year_str) or not is_valid_iso_week(iso_week_str):
            raise ValueError(
                f"invalid file path: '{file_path}'; expected: 'download-stats-weekly/<iso_year>/<iso_week>.json'"
            )
        year_week_file = (int(iso_year_str), int(iso_week_str))
        if year_week_file not in periods_in_ducklake:
            print(f"fetching data from {file_path}...")
            new_records.extend(get_download_stats_from_file(s3_client, file_path))

    # update ducklake
    if new_records:
        with DuckLakeConnection() as con:
            con.execute(
                f"""
                CREATE TABLE
                    IF NOT EXISTS {EXTENSION_DOWNLOADS_TABLE} (
                        year USMALLINT,
                        week UTINYINT,
                        extension_name VARCHAR,
                        downloads UINTEGER,
                        last_update TIMESTAMP_S,
                    )
                """
            )
            con.append_table(EXTENSION_DOWNLOADS_TABLE, new_records)
            print(f"inserted {len(new_records)} records to table '{EXTENSION_DOWNLOADS_TABLE}'.")
    else:
        print("no new extension stats to store")


def is_valid_iso_week(s: str):
    return s.isnumeric() and int(s) >= 1 and int(s) <= 53


def is_valid_iso_year(s: str):
    return bool(re.fullmatch(r"\d{4}", s))


def get_s3_file_paths(s3):
    # validations
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        raise ValueError(f"failed to connect to bucket: '{S3_BUCKET}'; error: {e.response['Error']}")
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_BUCKET_DIR)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise ValueError(
            f"Request 'list_objects_v2' failed with status: {response['ResponseMetadata']['HTTPStatusCode']}"
        )
    if not 'Contents' in response:
        raise ValueError(f"directory '{S3_BUCKET_DIR}' not found in bucket '{S3_BUCKET}'")
    file_paths = [obj['Key'] for obj in response['Contents']]
    if file_paths == []:
        raise ValueError(f"no files found in directory '{S3_BUCKET_DIR}' in bucket '{S3_BUCKET}'")
    return file_paths


def get_download_stats_from_file(s3_client, file_path):
    iso_year_str, _, iso_week_str = (
        file_path.removeprefix('download-stats-weekly/').removesuffix('.json').partition('/')
    )
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_path)
    content: dict = json.loads(response['Body'].read().decode('utf-8'))
    if '_last_update' not in content:
        raise ValueError(f"field '_last_update' not found in file {file_path}")
    download_stats = []
    last_update = content['_last_update']
    for key in content.keys():
        if key != '_last_update':
            extension_name = key
            downloads = content[extension_name]
            record = OrderedDict(
                year=int(iso_year_str),
                week=int(iso_week_str),
                extension_name=extension_name,
                downloads=downloads,
                last_update=last_update,
            )
            download_stats.append(record)
    return download_stats


if __name__ == "__main__":
    run()
