# CI Dashboard

## Setup

### Create a postgres-s3-ducklake with DuckDB CLI
- create a new bucket at Amazon S3, to serve as data store for the ducklake. Get the following vars:
    - `AWS_ACCESS_KEY_ID`
    - `AWS_SECRET_ACCESS_KEY`
    - `AWS_REGION`
    - `DUCKLAKE_S3_BUCKET` (the path of the bucket, e.g. `s3:duckdb-ci-dashboard-lake/`)
- create a Postgres server, e.g. at https://neon.com/ to serve as catalog for the ducklake. Get the following vars, (mentioned in the `connection string`):
    - `DUCKLAKE_HOST`
    - `DUCKLAKE_USER`
    - `DUCKLAKE_DB_PASSWORD`
- for convenience and local testing, add the vars mentioned above to an `.env` file (gitignored) and run `./utils/create_persistent_secrets.sh` to create [persistent secrets](https://duckdb.org/docs/stable/configuration/secrets_manager) to connect to the ducklake. Note that secrets are stored in `~/.duckdb/stored_secrets`.

### Connecting to the ducklake
- to connect to the ducklake with the credentials created above:
```sql
ATTACH 'ducklake:ducklake_secret' AS my_ducklake (READ_ONLY);
USE my_ducklake;
```
The ducklake can be used as regular database, e.g `PRAGMA show_tables;` (no result if the ducklake is still empty).

- to connect to the catalog database (e.g. to see the metadata):
```sql
ATTACH '' AS postgres_db (TYPE postgres, SECRET pg_secret, READ_ONLY);
USE postgres_db;
SELECT * FROM ducklake_metadata;
```

- to directly query the data store (with the credentials created above) e.g.:
```sql
SELECT * FROM glob('s3://duckdb-ci-dashboard-lake/**/*');
```


## data feeds

### `collect_ci_metrics.py`
Script that fetches and stores completed CI runs from: https://api.github.com/repos/duckdb/duckdb/actions/runs

Note that only consecutive 'completed' runs are stored.
After an initial run the script will only add new completed runs ('append only').
