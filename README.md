# DuckDB Dev Dashboard
This repository contains the code to operate the `duckdb-dev-dashboard`:
https://duckdblabs.github.io/duckdb-dev-dashboard

The tech stack:
- back-end: ducklake, with postgres catalog, and storage on cloudflare R2
- front-end: evidence (https://docs.evidence.dev)
- hosted: Github Pages
- updates: periodically via Github actions cron

## Setup

### Create a postgres-r2-ducklake with DuckDB CLI
- create a bucket at Cloudflare R2, to serve as data store for the ducklake. Get the following vars:
    - `DUCKLAKE_STORAGE_S3_KEY_ID`
    - `DUCKLAKE_STORAGE_S3_SECRET`
    - `DUCKLAKE_STORAGE_S3_ENDPOINT` (the path of the bucket, e.g. `r2://my-bucket/`)
    - `DUCKLAKE_STORAGE_R2_ACCOUNT_ID`
- create a Postgres server, e.g. at https://neon.com/ to serve as catalog for the ducklake. Get the following vars, (mentioned in the `connection string`):
    - `DUCKLAKE_CATALOG_PG_HOST`
    - `DUCKLAKE_CATALOG_PG_USER`
    - `DUCKLAKE_CATALOG_PG_PASSWORD`
- for convenience and local testing, add the vars mentioned above to a `.env` file (gitignored) and run `make secrets` to create [persistent secrets](https://duckdb.org/docs/stable/configuration/secrets_manager) to connect to the ducklake. Note that secrets are stored in `~/.duckdb/stored_secrets`.
- note that the front end is hosted on GitHub pages: https://docs.evidence.dev/deployment/self-host/github-pages

### Second lake: benchmark results (read-only)
The benchmarks dashboard reads a *second*, independent DuckLake, owned by the benchmark harness
(`scripts/engineering/benchmark` in `duckdb-internal`):
- catalog: `s3://duckdb-benchmark-lake/results.ducklake` — a DuckDB **database file**, not postgres
- data: `s3://duckdb-benchmark-lake/data/`, region `eu-central-1`

Because the catalog is a remote database file it can only be attached `READ_ONLY`; this repo never
writes to it. Set the following vars (read-only S3 credentials are sufficient, and are all this repo
should hold):
- `BENCHMARK_LAKE_S3_KEY_ID`
- `BENCHMARK_LAKE_S3_SECRET`

Both are required: `make secrets` fails if either is missing, so a missing credential surfaces there
rather than leaving the benchmarks dashboard silently stale. `make secrets` creates
`benchmark_s3_secret` and `benchmark_ducklake_secret` from them. The paths and region have defaults
and only need setting to point at a different bucket: `BENCHMARK_LAKE_BUCKET`,
`BENCHMARK_LAKE_CATALOG`, `BENCHMARK_LAKE_DATA_PATH`, `BENCHMARK_LAKE_REGION`.

To connect to it manually:
```sql
ATTACH 'ducklake:benchmark_ducklake_secret' AS bench (READ_ONLY);
USE bench;
SELECT benchmark, benchmark_name, scale_factor, count(*) FROM runs WHERE NOT is_test GROUP BY ALL;
```

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
SELECT * FROM glob('r2://my-r2-bucket/**/*');
```

## Adding Dashboards
Creating a dashboard requires the following steps:
- create a data feed (python script); this will run periodically and add should create and update the data in the ducklake required for the dashboard
- define a 'source' in evidence; a subdirectory under `./evidence/sources`
- define a 'page' in evidence; a markdown file under `./evidence/pages`

### defining data feeds
Data feeds are scripts that periodically store data in the ducklake
- all data feeds are python packages under `./feeds/` and will be run by `run_feeds.py` (via `make run_feeds`)
- to add a data feed, add python script (single file package) in a directory under `./feeds/` and update `run_feeds.py`
- data feeds should create the data table on first run
- the general lay-out of a data feed can be as follows:
```python
data = my_func_to_fetch_data_from_somewhere()

from utils.ducklake import DuckLakeConnection
with DuckLakeConnection(dl_secret) as con:
    con.execute(<<< sql statments to create tables, add records, etc... >>>)
```
- to locally test data feeds, the following the following make targets are available:
  - `make sync_local` - makes a local copy from production (both catalog and data)
  - `make run_feeds_local` - stores the fetched data in the local copy of the ducklake.

### defining sources
The evidence front-end (see [./evidence/README.md](/evidence/README.md)) can not directly serve from the ducklake, therefore `.duckdb` files will be created as in-between step.
This is not ideal, since evidence itself also copies the data to convert the data into parquet.
Therfore (for now) there are 2 build steps:
- `make generate_sources`:  converts data in the ducklake into `.duckdb` persistent file
- `make build`: converts `.duckdb` into `.parquet` and builds the front-end

Steps to define a new source:
- initial step to create a new source, see: https://docs.evidence.dev/core-concepts/data-sources/duckdb/
    - run `make dev` to spawn the front-end (make sure evidence is installed locally, see [./evidence/README.md](/evidence/README.md))
    - following the steps in the link above will create a subdirectory under `./evidence/sources`
- add the source to `evidence/sources/sources.json`, to specify which tables from the ducklake are needed.
    - a source may also read from a *different* lake, and materialize an aggregate instead of copying a table whole:
        - `lake_secret` - name of the ducklake secret to attach (default: `ducklake_secret`)
        - `read_only` - attach the lake `READ_ONLY` instead of with `AUTOMATIC_MIGRATION` (default: `false`)
        - `derived_tables` - `[{name, sql_file}]`; the SQL file is run against the lake and its result is stored as table `name`. Use this when the raw table is far too large to ship to the browser: the benchmarks source turns one row per query per warm run per metric into one row per benchmark run. These SQL files live in `./benchmark_derived_tables/`, deliberately *outside* `evidence/sources/` - evidence treats every `.sql` under a source directory as a source query.
    - to refresh only some sources (e.g. when you do not have credentials for every lake): `python3 -m evidence.sources.generate_sources ci_metrics extension_downloads`
- run `make generate_sources`, this should create the `.duckdb` file (which is .gitignored, but needed for local testing).
- add one or more `.sql` files to select the data relevant for the dashboard


### defining dashboard pages
Define dashboard pages in `./evidence/pages`.
- To use the `.sql` files created in previous step, see https://docs.evidence.dev/core-concepts/queries/
- Bars / Charts and other components, see: https://docs.evidence.dev/core-concepts/components/
- locally test with `make dev`

## example: ci stats dashboard to monitor CI
- data feed: `feeds/collect_ci_metrics.py` #todo update
- source: `evidence/sources/ci_metrics/`
- page: `evidence/pages/ci-stats.md`

The data feed that fetches and stores completed CI runs from:
- https://api.github.com/repos/duckdb/duckdb/actions/workflows
- https://api.github.com/repos/duckdb/duckdb/actions/runs
- https://api.github.com/repos/duckdb/duckdb/actions/runs/{RUN_ID}/jobs

Note that only consecutive 'completed' runs are stored.
After an initial run the script will add new completed runs ('append only').
