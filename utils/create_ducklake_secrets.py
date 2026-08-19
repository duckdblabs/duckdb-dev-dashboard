"""
create persistent secrets for DuckLake
"""

from dotenv import load_dotenv
import os
import duckdb

load_dotenv()


# postgres secret for ducklake catalog
Q_CATALOG_SECRET = f"""
    CREATE OR REPLACE PERSISTENT SECRET pg_secret (
        TYPE postgres,
        HOST '{os.getenv('DUCKLAKE_CATALOG_PG_HOST')}',
        PORT 5432,
        DATABASE ducklake_catalog,
        USER '{os.getenv('DUCKLAKE_CATALOG_PG_USER')}',
        PASSWORD '{os.getenv('DUCKLAKE_CATALOG_PG_PASSWORD')}'
    )
    """

# cloudflare r2 secret for ducklake storage
Q_STORAGE_SECRET = f"""
    CREATE OR REPLACE PERSISTENT SECRET r2_secret (
        TYPE r2,
        ACCOUNT_ID '{os.getenv('DUCKLAKE_STORAGE_R2_ACCOUNT_ID')}',
        KEY_ID '{os.getenv('DUCKLAKE_STORAGE_S3_KEY_ID')}',
        SECRET '{os.getenv('DUCKLAKE_STORAGE_S3_SECRET')}',
        REGION 'auto',
        SCOPE '{os.getenv('DUCKLAKE_STORAGE_S3_ENDPOINT')}'
    )
    """

# s3 bucket (staging)
# Q_STORAGE_SECRET_STAGING = f"""
#     CREATE OR REPLACE PERSISTENT SECRET s3_staging_test (
#         TYPE s3,
#         PROVIDER config,
#         KEY_ID '{os.getenv('DUCKLAKE_STORAGE_S3_KEY_ID')}',
#         SECRET '{os.getenv('DUCKLAKE_STORAGE_S3_SECRET')}',
#         REGION 'eu-north-1',
#         SCOPE '{os.getenv('DUCKLAKE_STORAGE_S3_ENDPOINT')}'
#     );
#     """

# ducklake connection secret (note: uses 'pg_secret' defined above)
Q_DUCKLAKE_SECRET = f"""
    CREATE OR REPLACE PERSISTENT SECRET ducklake_secret (
        TYPE ducklake,
        METADATA_PATH '',
        DATA_PATH '{os.getenv('DUCKLAKE_STORAGE_S3_ENDPOINT')}',
        METADATA_PARAMETERS MAP {{'TYPE': 'postgres', 'SECRET': 'pg_secret'}}
    )
    """


# ---------------------------------------------------------------------------
# benchmark results lake (read-only)
#
# A second, independent DuckLake, written by scripts/engineering/benchmark in duckdb-internal.
# Its catalog is a DuckDB *database file* published to S3, not a postgres database - hence no
# METADATA_PARAMETERS, and readers must attach it READ_ONLY (a remote database file cannot be
# written).
#
# BENCHMARK_LAKE_S3_KEY_ID and BENCHMARK_LAKE_S3_SECRET are required (see validate_env), so a
# missing credential fails 'make secrets' loudly rather than leaving the benchmarks dashboard
# silently stale. The paths and region below have defaults and only need setting to point at a
# different bucket.
# ---------------------------------------------------------------------------

BENCHMARK_LAKE_BUCKET = os.getenv('BENCHMARK_LAKE_BUCKET', 's3://duckdb-benchmark-lake')
BENCHMARK_LAKE_CATALOG = os.getenv('BENCHMARK_LAKE_CATALOG', f'{BENCHMARK_LAKE_BUCKET}/results.ducklake')
BENCHMARK_LAKE_DATA_PATH = os.getenv('BENCHMARK_LAKE_DATA_PATH', f'{BENCHMARK_LAKE_BUCKET}/data/')
BENCHMARK_LAKE_REGION = os.getenv('BENCHMARK_LAKE_REGION', 'eu-central-1')

# s3 secret for the benchmark lake (catalog file and data files are in the same bucket)
# note: SCOPE is not optional here; an unscoped s3 secret becomes the default for every s3:// path
# in the process. 'r2_secret' is scoped to an r2:// path, so the two never compete either way.
Q_BENCHMARK_S3_SECRET = f"""
    CREATE OR REPLACE PERSISTENT SECRET benchmark_s3_secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{os.getenv('BENCHMARK_LAKE_S3_KEY_ID')}',
        SECRET '{os.getenv('BENCHMARK_LAKE_S3_SECRET')}',
        REGION '{BENCHMARK_LAKE_REGION}',
        SCOPE '{BENCHMARK_LAKE_BUCKET}'
    )
    """

# ducklake connection secret (note: uses 'benchmark_s3_secret' defined above for bucket access)
Q_BENCHMARK_DUCKLAKE_SECRET = f"""
    CREATE OR REPLACE PERSISTENT SECRET benchmark_ducklake_secret (
        TYPE ducklake,
        METADATA_PATH '{BENCHMARK_LAKE_CATALOG}',
        DATA_PATH '{BENCHMARK_LAKE_DATA_PATH}'
    )
    """


def create_ducklake_secrets():
    with duckdb.connect() as con:
        con.execute(Q_CATALOG_SECRET)
        con.execute(Q_STORAGE_SECRET)
        con.execute(Q_DUCKLAKE_SECRET)
        con.execute(Q_BENCHMARK_S3_SECRET)
        con.execute(Q_BENCHMARK_DUCKLAKE_SECRET)


def validate_env():
    required_env_vars = [
        "DUCKLAKE_STORAGE_S3_KEY_ID",
        "DUCKLAKE_STORAGE_S3_SECRET",
        "DUCKLAKE_STORAGE_S3_ENDPOINT",
        "DUCKLAKE_STORAGE_R2_ACCOUNT_ID",
        "DUCKLAKE_CATALOG_PG_PASSWORD",
        "DUCKLAKE_CATALOG_PG_HOST",
        "DUCKLAKE_CATALOG_PG_USER",
        "BENCHMARK_LAKE_S3_KEY_ID",
        "BENCHMARK_LAKE_S3_SECRET",
    ]
    for env_var in required_env_vars:
        if env_var not in os.environ.keys():
            raise ValueError(f"Env variable '{env_var}' is missing!")
        if os.getenv(env_var) == "":
            raise ValueError(f"Env variable '{env_var}' is empty!")


if __name__ == "__main__":
    validate_env()
    create_ducklake_secrets()
