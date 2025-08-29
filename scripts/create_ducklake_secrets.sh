#!/usr/bin/env bash

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# ducklake storage credentials (cloudflare r2)
if [ -z "$S3_KEY_ID" ];             then echo "Error: S3_KEY_ID is not set."; exit 1; fi
if [ -z "$S3_SECRET_KEY" ];         then echo "Error: S3_SECRET_KEY is not set."; exit 1; fi
if [ -z "$S3_ENDPOINT" ];           then echo "Error: S3_ENDPOINT is not set."; exit 1; fi
if [ -z "$R2_ACCOUNT_ID" ];         then echo "Error: R2_ACCOUNT_ID is not set."; exit 1; fi

# ducklake meta data credentials
if [ -z "$DUCKLAKE_HOST" ];         then echo "Error: DUCKLAKE_HOST is not set."; exit 1; fi
if [ -z "$DUCKLAKE_USER" ];         then echo "Error: DUCKLAKE_USER is not set."; exit 1; fi
if [ -z "$DUCKLAKE_DB_PASSWORD" ];  then echo "Error: DUCKLAKE_DB_PASSWORD is not set."; exit 1; fi


duckdb -c "
INSTALL postgres;
INSTALL ducklake;
INSTALL httpfs;

CREATE OR REPLACE PERSISTENT SECRET r2_secret (
    TYPE r2,
    ACCOUNT_ID '$R2_ACCOUNT_ID',
    KEY_ID '$S3_KEY_ID',
    SECRET '$S3_SECRET_KEY',
    REGION 'auto',
    SCOPE '$S3_ENDPOINT'
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
	  DATA_PATH '$S3_ENDPOINT',
	  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_secret'}
);
"
