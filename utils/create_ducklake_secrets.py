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
        HOST '{os.getenv('DUCKLAKE_HOST')}',
        PORT 5432,
        DATABASE ducklake_catalog,
        USER '{os.getenv('DUCKLAKE_USER')}',
        PASSWORD '{os.getenv('DUCKLAKE_DB_PASSWORD')}'
    )
    """

# cloudflare r2 secret for ducklake storage
Q_STORAGE_SECRET = f"""
    CREATE OR REPLACE PERSISTENT SECRET r2_secret (
        TYPE r2,
        ACCOUNT_ID '{os.getenv('R2_ACCOUNT_ID')}',
        KEY_ID '{os.getenv('S3_KEY_ID')}',
        SECRET '{os.getenv('S3_SECRET_KEY')}',
        REGION 'auto',
        SCOPE '{os.getenv('S3_ENDPOINT')}'
    )
    """

# ducklake connection secret (note: uses 'pg_secret' defined above)
Q_DUCKLAKE_SECRET = f"""
    CREATE OR REPLACE PERSISTENT SECRET ducklake_secret (
        TYPE ducklake,
        METADATA_PATH '',
        DATA_PATH '{os.getenv('S3_ENDPOINT')}',
        METADATA_PARAMETERS MAP {{'TYPE': 'postgres', 'SECRET': 'pg_secret'}}
    )
    """


def create_ducklake_secrets():
    with duckdb.connect() as con:
        con.execute(Q_CATALOG_SECRET)
        con.execute(Q_STORAGE_SECRET)
        con.execute(Q_DUCKLAKE_SECRET)


def validate_env():
    required_env_vars = [
        "S3_KEY_ID",
        "S3_SECRET_KEY",
        "S3_ENDPOINT",
        "DUCKLAKE_DB_PASSWORD",
        "DUCKLAKE_HOST",
        "DUCKLAKE_USER",
    ]
    for env_var in required_env_vars:
        if env_var not in os.environ.keys():
            raise ValueError(f"Env variable '{env_var}' is missing!")
        if os.getenv(env_var) == "":
            raise ValueError(f"Env variable '{env_var}' is empty!")


if __name__ == "__main__":
    validate_env()
    create_ducklake_secrets()
