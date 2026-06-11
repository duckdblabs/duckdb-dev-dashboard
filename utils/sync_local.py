# creates local copy of the ducklake for testing and debug
# - copies the postgres catalog into local duckdb catalog file
# - copies parquet data from cloudflare r2 to local dir


from datetime import datetime
from dotenv import load_dotenv
import duckdb
import os
from pathlib import Path
import subprocess

load_dotenv()

DUCKDB_CATALOG = "my_ducklake.ducklake"
POSTGRES_CATALOG_DB = "ducklake_catalog"


def create_local_catalog(sync_dir: Path, local_data_path: Path):
    with duckdb.connect() as con:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"""
            CREATE SECRET pg_reader (
                TYPE postgres,
                HOST '{os.getenv('DUCKLAKE_CATALOG_PG_HOST')}',
                PORT 5432,
                DATABASE {POSTGRES_CATALOG_DB},
                USER '{os.getenv('DUCKLAKE_CATALOG_PG_USER')}',
                PASSWORD '{os.getenv('DUCKLAKE_CATALOG_PG_PASSWORD')}'
            );
        """)
        # attach source and destination databases
        con.execute(f"ATTACH '' AS postgres_db (TYPE postgres, SCHEMA 'public', SECRET pg_reader, READ_ONLY);")
        con.execute(f"ATTACH '{sync_dir}/{DUCKDB_CATALOG}' AS my_ducklake;")
        # copy catalog tables into local catalog file
        catalog_tables = [tup[0] for tup in con.sql(f"show tables from postgres_db").fetchall()]
        for table in catalog_tables:
            print(f"syncing catalog table: {table} ...")
            con.execute(f"CREATE TABLE my_ducklake.{table} AS FROM postgres_db.{table}")
        # update data_path in ducklake_metadata: NOTE: absolute path with trailing slash!
        data_path = str(local_data_path.absolute()) + '/'
        print(f"update metadata: set data_path to: '{data_path}'")
        con.execute(f"update my_ducklake.ducklake_metadata set value = '{data_path}' where key = 'data_path'")


# make sure r2 credentials are available in profile [r2-extensions] in ~/.aws/credentials
def create_local_storage(local_data_path: Path):
    print(f"downloading parquet files...", flush=True)
    result = subprocess.run(
        [
            "aws", "s3", "sync",
            "s3://ducklake-dogfood", local_data_path,
            "--profile", "r2-extensions",
            "--endpoint", f"https://{os.getenv('DUCKLAKE_STORAGE_R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    print(result.stdout)


def create_local_secret(sync_dir: Path):
    abs_catalog_path = (sync_dir / DUCKDB_CATALOG).absolute()
    with duckdb.connect() as con:
        con.execute(f"""
            CREATE OR REPLACE PERSISTENT SECRET ducklake_secret_local (
                TYPE ducklake,
                METADATA_PATH '{abs_catalog_path}'
            );
        """)
    print(f"updated 'ducklake_secret_local', it now points to: {abs_catalog_path}")


def main():
    root_dir = Path('local_copy')
    sync_dir = root_dir / f"dl_{datetime.now().strftime("%Y%m%d_%H%M%S")}"
    local_data_path = sync_dir / 'r2_data'
    local_data_path.mkdir(parents=True)
    create_local_catalog(sync_dir, local_data_path)
    create_local_storage(local_data_path)
    create_local_secret(sync_dir)

    connection_str = (
        "install ducklake; load ducklake;\n"
        f"attach 'ducklake:{sync_dir}/{DUCKDB_CATALOG}' as my_ducklake; use my_ducklake;\n"
        "-- show tables;\n\n"
        "-- or:\n"
        "-- attach 'ducklake:ducklake_secret_local' as my_ducklake; use my_ducklake;\n"
    )
    connect_sql_file = sync_dir / 'connect.sql'
    connect_sql_file.write_text(connection_str)

    print(f"finished creating local copy in dir: ./{sync_dir}")
    print(f"to connect, use statements in ./{connect_sql_file}")


if __name__ == "__main__":
    main()
