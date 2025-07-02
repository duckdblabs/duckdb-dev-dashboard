#!/usr/bin/env bash

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

CREATE OR REPLACE TABLE ci_runs AS FROM my_ducklake.ci_runs;
"
