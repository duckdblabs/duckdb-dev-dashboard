#!/usr/bin/env bash

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

if [ -z "$AWS_ACCESS_KEY_ID" ];     then echo "Error: AWS_ACCESS_KEY_ID is not set."; exit 1; fi
if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then echo "Error: AWS_SECRET_ACCESS_KEY is not set."; exit 1; fi
if [ -z "$AWS_REGION" ];            then echo "Error: AWS_REGION is not set."; exit 1; fi
if [ -z "$DUCKLAKE_HOST" ];         then echo "Error: DUCKLAKE_HOST is not set."; exit 1; fi
if [ -z "$DUCKLAKE_USER" ];         then echo "Error: DUCKLAKE_USER is not set."; exit 1; fi
if [ -z "$DUCKLAKE_DB_PASSWORD" ];  then echo "Error: DUCKLAKE_DB_PASSWORD is not set."; exit 1; fi


duckdb ./evidence/sources/ci_metrics/ci_metrics.duckdb -c "
INSTALL ducklake;
INSTALL postgres;

CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID '$AWS_ACCESS_KEY_ID',
    SECRET '$AWS_SECRET_ACCESS_KEY',
    REGION '$AWS_REGION'
);

ATTACH 'ducklake:postgres:dbname=ducklake_catalog
        password=$DUCKLAKE_DB_PASSWORD
        host=$DUCKLAKE_HOST
        user=$DUCKLAKE_USER'
    AS my_ducklake
    (DATA_PATH 's3://duckdb-ci-dashboard-lake/', READ_ONLY);

CREATE OR REPLACE TABLE ci_workflows AS FROM my_ducklake.ci_workflows;
CREATE OR REPLACE TABLE ci_runs AS FROM my_ducklake.ci_runs;
CREATE OR REPLACE TABLE ci_jobs AS FROM my_ducklake.ci_jobs;
"
