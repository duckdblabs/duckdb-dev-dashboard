# DuckDB Dev Dashboard
This repository contains the code to operate the `duckdb-dev-dashboard`:
https://duckdblabs.github.io/duckdb-dev-dashboard

The tech stack:
- back-end: ducklake, with postgres catalog, and storage on amazon s3
- front-end: evidence (https://docs.evidence.dev)
- hosted: Github Pages
- updates: periodically via Github actions cron

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
- for convenience and local testing, add the vars mentioned above to an `.env` file (gitignored) and run `./scripts/create_persistent_secrets.sh` to create [persistent secrets](https://duckdb.org/docs/stable/configuration/secrets_manager) to connect to the ducklake. Note that secrets are stored in `~/.duckdb/stored_secrets`.
- note that the front end is hosted on GitHub pages: https://docs.evidence.dev/deployment/self-host/github-pages

### Testing set-up: Connecting to the ducklake
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

## Adding Dashboards
Creating a dashboard requires the following steps:
- create a data feed (python script); this will run periodically and add should create and update the data in the ducklake required for the dashboard
- define a 'source' in evidence; a subdirectory under `./evidence/sources`
- define a 'page' in evidence; a markdown file under `./evidence/pages`

### defining data feeds
Data feeds are scripts that periodically store data in the ducklake
- all data feeds are python files under `./feeds/` and will be run by `run_feeds.py` (via `make run_feeds`)
- the general lay-out of a data feed can be as follows
```python
data = my_func_to_fetch_data_from_somewhere()

from utils.ducklake import DuckLakeConnection
with DuckLakeConnection() as con:
    con.execute(<<< sql statments to create tables, add records, etc... >>>)
```

### defining sources
The evidence front-end (see [./evidence/README.md](/evidence/README.md)) can not directly serve from the ducklake, therefore `.duckdb` files will be created as in-between step.
This is not ideal, since evidence itself also copies the data to convert the data into parquet.
Therfore (for now) there are 2 build steps:
- `make generate_sources`:  converts data in the ducklake into `.duckdb` persistent file
- `make build`: converts `.duckdb` into `.parquet` and builds the front-end

Steps to define a new source:
- initial step to create a new source, see: https://docs.evidence.dev/core-concepts/data-sources/duckdb/
    - run `make dev` to spawn the front-end
    - following the steps in the link above will create a subdirectory under `./evidence/sources`
- update `./scripts/generate_sources.sh` to make sure a `.duckdb` file is created in this subdirectory with all data required for the dashboard
- add one or more `.sql` files to select the data relevant for the dashboard


### defining dashboard pages
Define dashboard pages in `./evidence/pages`.
- To use the `.sql` files created in previous step, see https://docs.evidence.dev/core-concepts/queries/
- Bars / Charts and other components, see: https://docs.evidence.dev/core-concepts/components/
- locally test with `make dev`

## example: ci stats dashboard to monitor CI
- data feed: `feeds/collect_ci_metrics.py`
- source: `evidence/sources/ci_metrics/`
- page: `evidence/pages/ci-stats.md`

The data feed that fetches and stores completed CI runs from:
- https://api.github.com/repos/duckdb/duckdb/actions/workflows
- https://api.github.com/repos/duckdb/duckdb/actions/runs
- https://api.github.com/repos/duckdb/duckdb/actions/runs/{RUN_ID}/jobs

Note that only consecutive 'completed' runs are stored.
After an initial run the script will add new completed runs ('append only').
