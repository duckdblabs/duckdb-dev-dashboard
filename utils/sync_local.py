# creates a local copy of the ducklake for testing and debug, in direcory ./local_copy
# - copies the postgres catalog to a local postgres instance
# - copies parquet data from cloudflare r2 to the local dir

# each run creates an independent, timestamped copy
# secret 'ducklake_secret_local' points at the most recent one.


from datetime import datetime
from dotenv import load_dotenv
import duckdb
import getpass
import os
from pathlib import Path
import shutil
import subprocess
import time

load_dotenv()

POSTGRES_CATALOG_DB = "ducklake_catalog"

BREW_PG_FORMULA = "postgresql@17"
LOCAL_PG_HOST = "localhost"
LOCAL_PG_PORT = 5432
LOCAL_PG_USER = getpass.getuser()


def validate_env():
    # read-only production credentials should be available, to make a local copy
    required_env_vars = [
        "DUCKLAKE_CATALOG_PG_HOST",
        "DUCKLAKE_CATALOG_PG_USER",
        "DUCKLAKE_CATALOG_PG_PASSWORD",
        "DUCKLAKE_STORAGE_R2_ACCOUNT_ID",
    ]
    for env_var in required_env_vars:
        if env_var not in os.environ.keys():
            raise ValueError(f"Env variable '{env_var}' is missing!")
        if os.getenv(env_var) == "":
            raise ValueError(f"Env variable '{env_var}' is empty!")


def require_pg_tools():
    missing = [tool for tool in ["pg_isready", "createdb", "dropdb", "pg_dump", "psql"] if shutil.which(tool) is None]
    if missing:
        raise RuntimeError(
            f"required postgres tools not found on PATH: {', '.join(missing)}"
        )


def ensure_local_pg():
    print(f"ensuring local postgres ({BREW_PG_FORMULA}) is running ...", flush=True)
    subprocess.run(["brew", "services", "start", BREW_PG_FORMULA], check=True)
    for _ in range(30):
        ready = subprocess.run(["pg_isready", "-h", LOCAL_PG_HOST, "-p", str(LOCAL_PG_PORT)])
        if ready.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("local postgres did not become ready in time")


def create_local_catalog(db_name: str, local_data_path: Path):
    require_pg_tools()
    ensure_local_pg()
    print(f"creating local database '{db_name}' ...", flush=True)
    subprocess.run(["createdb", "-h", LOCAL_PG_HOST, "-p", str(LOCAL_PG_PORT), db_name], check=True)

    # copy the production catalog verbatim into the local db
    print(f"copying catalog '{POSTGRES_CATALOG_DB}' from production into '{db_name}' ...", flush=True)
    dump_env = {**os.environ, "PGPASSWORD": os.getenv("DUCKLAKE_CATALOG_PG_PASSWORD", "")}
    dump = subprocess.Popen(
        [
            "pg_dump",
            "-h", os.getenv("DUCKLAKE_CATALOG_PG_HOST"),
            "-p", "5432",
            "-U", os.getenv("DUCKLAKE_CATALOG_PG_USER"),
            "-d", POSTGRES_CATALOG_DB,
            "--no-owner", "--no-privileges",
        ],
        stdout=subprocess.PIPE,
        env=dump_env,
    )
    restore = subprocess.Popen(
        ["psql", "-h", LOCAL_PG_HOST, "-p", str(LOCAL_PG_PORT), "-d", db_name, "-v", "ON_ERROR_STOP=1", "-q"],
        stdin=dump.stdout,
    )
    dump.stdout.close()  # let pg_dump receive SIGPIPE if psql exits early
    restore.communicate()
    dump.wait()
    if dump.returncode != 0 or restore.returncode != 0:
        raise RuntimeError(f"catalog copy failed (pg_dump exit={dump.returncode}, psql exit={restore.returncode})")

    # update data_path in ducklake_metadata: NOTE: absolute path with trailing slash!
    data_path = str(local_data_path.absolute()) + '/'
    print(f"update metadata: set data_path to: '{data_path}'")
    subprocess.run(
        [
            "psql", "-h", LOCAL_PG_HOST, "-p", str(LOCAL_PG_PORT), "-d", db_name, "-v", "ON_ERROR_STOP=1",
            "-c", f"UPDATE ducklake_metadata SET value = '{data_path}' WHERE key = 'data_path'",
        ],
        check=True,
    )


# make sure r2 credentials are available in profile [r2-extensions] in ~/.aws/credentials
def create_local_storage(local_data_path: Path):
    print("downloading parquet files...", flush=True)
    result = subprocess.run(
        [
            "aws", "s3", "sync",
            "s3://ducklake-dogfood", local_data_path,
            "--profile", "r2-extensions",
            "--endpoint", f"https://{os.getenv('DUCKLAKE_STORAGE_R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(result.stdout)


def create_local_secrets(db_name: str):
    with duckdb.connect() as con:
        con.execute(f"""
            CREATE OR REPLACE PERSISTENT SECRET pg_secret_local (
                TYPE postgres,
                HOST '{LOCAL_PG_HOST}',
                PORT {LOCAL_PG_PORT},
                DATABASE {db_name},
                USER '{LOCAL_PG_USER}'
            );
        """)
        con.execute(f"""
            CREATE OR REPLACE PERSISTENT SECRET ducklake_secret_local (
                TYPE ducklake,
                METADATA_PATH '',
                METADATA_PARAMETERS MAP {{'TYPE': 'postgres', 'SECRET': 'pg_secret_local'}}
            );
        """)
    print(f"updated 'ducklake_secret_local', it now points at local postgres db '{db_name}'")


def main():
    validate_env()
    root_dir = Path('local_copy')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    sync_dir = root_dir / f"pg_{timestamp}"
    local_data_path = sync_dir / 'r2_data'
    local_data_path.mkdir(parents=True)
    db_name = f"ducklake_{timestamp}"

    create_local_catalog(db_name, local_data_path)
    create_local_storage(local_data_path)
    create_local_secrets(db_name)

    connection_str = (
        "-- to connect to the local ducklake:\n"
        "install ducklake; load ducklake; install postgres; load postgres;\n"
        "attach 'ducklake:ducklake_secret_local' as my_ducklake; use my_ducklake;\n\n"
        "-- the secret contains credential for:\n"
        f"--   data: {local_data_path}\n"
        f"--   catalog: {db_name}\n\n"
        "-- to list all ducklake catalogs in the cluster:\n"
        "--   psql -h localhost -d postgres -c \"SELECT datname FROM pg_database WHERE datname LIKE 'ducklake\\_%' ORDER BY datname;\"\n\n"
        "-- to drop it if it is no longer needed:\n"
        f"--   dropdb -h localhost {db_name}"
    )
    connect_sql_file = sync_dir / 'connect.sql'
    connect_sql_file.write_text(connection_str)

    print(f"finished creating local copy in dir: ./{sync_dir} , with catalog: {db_name}")
    print(f"to connect, use statements in ./{connect_sql_file}")


if __name__ == "__main__":
    main()
