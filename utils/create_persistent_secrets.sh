#!/usr/bin/env bash

DUCKLAKE_S3_BUCKET="s3://duckdb-ci-dashboard-lake/"

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

if [ -z "$AWS_ACCESS_KEY_ID" ];     then echo "Error: AWS_ACCESS_KEY_ID is not set."; exit 1; fi
if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then echo "Error: AWS_SECRET_ACCESS_KEY is not set."; exit 1; fi
if [ -z "$AWS_REGION" ];            then echo "Error: AWS_REGION is not set."; exit 1; fi
if [ -z "$DUCKLAKE_S3_BUCKET" ];    then echo "Error: DUCKLAKE_S3_BUCKET is not set."; exit 1; fi
if [ -z "$DUCKLAKE_HOST" ];         then echo "Error: DUCKLAKE_HOST is not set."; exit 1; fi
if [ -z "$DUCKLAKE_USER" ];         then echo "Error: DUCKLAKE_USER is not set."; exit 1; fi
if [ -z "$DUCKLAKE_DB_PASSWORD" ];  then echo "Error: DUCKLAKE_DB_PASSWORD is not set."; exit 1; fi

duckdb -c "
INSTALL postgres;
INSTALL ducklake;

CREATE OR REPLACE PERSISTENT SECRET s3_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID '$AWS_ACCESS_KEY_ID',
    SECRET '$AWS_SECRET_ACCESS_KEY',
    REGION '$AWS_REGION',
    SCOPE '$S3_BUCKET'
);

CREATE OR REPLACE PERSISTENT SECRET pg_secret (
    TYPE postgres,
    HOST '$DUCKLAKE_HOST',
    PORT 5432,
    DATABASE ducklake_catalog,
    USER '$DUCKLAKE_USER',
    PASSWORD '$DUCKLAKE_DB_PASSWORD'
);

CREATE OR REPLACE PERSISTENT SECRET ducklake_secret (
	  TYPE ducklake,
	  METADATA_PATH '',
	  DATA_PATH '$S3_BUCKET',
	  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_secret'}
);
"
