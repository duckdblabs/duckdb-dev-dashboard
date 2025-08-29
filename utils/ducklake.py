from collections import OrderedDict
import duckdb
from dotenv import load_dotenv
import json
import os
import tempfile
import psycopg2
from urllib.parse import urlparse


class DuckLakeConnection:
    def __init__(self, storage_type=""):
        load_dotenv()
        for env_var in [
            "S3_KEY_ID",
            "S3_SECRET_KEY",
            "S3_ENDPOINT",
            "DUCKLAKE_DB_PASSWORD",
            "DUCKLAKE_HOST",
            "DUCKLAKE_USER",
        ]:
            if env_var not in os.environ.keys():
                raise ValueError(f"Env variable '{env_var}' is missing!")
            if os.getenv(env_var) == "":
                raise ValueError(f"Env variable '{env_var}' is empty!")
        self.con = None
        self.catalog_db_name = 'ducklake_catalog'
        self.ducklake_db_alias = 'my_ducklake'
        if storage_type and storage_type not in ['s3', 'r2']:
            raise ValueError(f"Invalid storage_type: '{storage_type}', should be 's3' or 'r2'")
        if storage_type == "":
            if os.getenv("S3_ENDPOINT").startswith('s3'):
                self.storage_type = 's3'
            else:
                self.storage_type = 'r2'
        else:
            self.storage_type = storage_type
        print(f"initializing ducklake connection with storage type: '{self.storage_type}'", flush=True)
        if self.storage_type == 'r2' and not os.getenv("S3_ENDPOINT").startswith("r2://"):
            self.storage_endpoint = self._convert_r2_endpoint(os.getenv("S3_ENDPOINT"))
        else:
            self.storage_endpoint = os.getenv("S3_ENDPOINT")
        self._create_catalog_db_if_not_exists()

    def __enter__(self):
        self.con = duckdb.connect()
        self.con.execute('INSTALL ducklake; LOAD ducklake;')
        self.con.execute('INSTALL postgres; LOAD postgres;')
        self.con.execute('INSTALL httpfs; LOAD httpfs;')
        self._create_storage_secret()
        self.con.execute(
            f"""
            ATTACH 'ducklake:postgres:dbname={self.catalog_db_name}
                password={os.getenv("DUCKLAKE_DB_PASSWORD")}
                host={os.getenv("DUCKLAKE_HOST")}
                user={os.getenv("DUCKLAKE_USER")}'
            AS {self.ducklake_db_alias}
            (DATA_PATH '{self.storage_endpoint}');
            """
        )
        self.con.execute(f"USE {self.ducklake_db_alias}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.con.close()

    def _convert_r2_endpoint(self, http_endpoint: str) -> str:
        # Convert an endpoint URL like:
        #   https://example.com/my-bucket/
        # into:
        #   r2://my-bucket/
        # this is needed because: "R2 secrets are only available when using URLs starting with r2://"
        # see: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api#r2-secrets
        bucket_path = urlparse(http_endpoint).path.strip("/")
        return f"r2://{bucket_path}/"

    def _create_catalog_db_if_not_exists(self):
        # create postgres catalog db (empty)
        CATALOG_DB_NAME = self.catalog_db_name
        con = psycopg2.connect(
            dbname="postgres",
            user=os.environ["DUCKLAKE_USER"],
            host=os.environ["DUCKLAKE_HOST"],
            password=os.environ["DUCKLAKE_DB_PASSWORD"],
        )
        con.autocommit = True
        try:
            with con.cursor() as cursor:
                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{CATALOG_DB_NAME}'")
                if not cursor.fetchone():
                    cursor.execute(f"CREATE DATABASE {CATALOG_DB_NAME}")
                    print(f"Ducklake catalog database created.")
        finally:
            con.close()

    def _create_storage_secret(self):
        if self.storage_type == 's3':
            s3_region = os.getenv("AWS_REGION")
            self.con.execute(
                f"""
                CREATE OR REPLACE SECRET s3_secret (
                    TYPE s3,
                    PROVIDER config,
                    KEY_ID '{os.getenv("S3_KEY_ID")}',
                    SECRET '{os.getenv("S3_SECRET_KEY")}',
                    {f"REGION '{s3_region}'," if s3_region else ''}
                    SCOPE '{self.storage_endpoint}'
                )
                """
            )
        elif self.storage_type == 'r2':
            if 'R2_ACCOUNT_ID' not in os.environ.keys():
                raise ValueError(f"Env variable 'R2_ACCOUNT_ID' is missing!")
            if len(os.getenv("R2_ACCOUNT_ID")) != 32:
                print(f"R2_ACCOUNT_ID has length: {len(os.getenv("R2_ACCOUNT_ID"))} (should be 32)!", flush=True)
                raise ValueError(f"R2_ACCOUNT_ID has length: {len(os.getenv("R2_ACCOUNT_ID"))} (should be 32)!")
            self.con.execute(
                f"""
                CREATE OR REPLACE SECRET r2_secret (
                    TYPE r2,
                    PROVIDER config,
                    KEY_ID '{os.getenv("S3_KEY_ID")}',
                    SECRET '{os.getenv("S3_SECRET_KEY")}',
                    ACCOUNT_ID '{os.getenv("R2_ACCOUNT_ID")}',
                    SCOPE '{self.storage_endpoint}'
                )
                """
            )
        else:
            raise ValueError(f"Unsupported storage_type: '{self.storage_type}, should be 'r2' or 's3'")

    def sql(self, sql_str):
        return self.con.sql(sql_str)

    def execute(self, sql_str):
        return self.con.execute(sql_str)

    def table_exists(self, table_name: str) -> bool:
        return self.con.sql(f"select 1 from duckdb_tables() where table_name='{table_name}' and database_name='{self.ducklake_db_alias}'").fetchone() == (1,)

    def table_empty(self, table_name: str) -> bool:
        return self.con.sql(f"select count(*) from {table_name}").fetchone() == (0,)

    def max_id(self, table_name: str):
        return self.con.sql(f"select max(id) from {table_name}").fetchone()[0]

    def create_table(self, table_name: str, records: list[OrderedDict]):
        json_str = f"[{',\n'.join([json.dumps(rec) for rec in records])}]"
        # work-around: use temp-file to utilize the type-sniffer
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as tmp:
            tmp.write(json_str)
            tmp.flush()
            self.con.execute(f"create table {table_name} as from read_json('{tmp.name}')")

    def append_table(self, table_name: str, records: list[OrderedDict]):
        json_str = f"[{',\n'.join([json.dumps(rec) for rec in records])}]"
        # work-around: use temp-file to utilize the type-sniffer
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as tmp:
            tmp.write(json_str)
            tmp.flush()
            self.con.execute(f"insert into {table_name} from read_json('{tmp.name}')")


# example usage:
# with DuckLakeConnection() as con:
#     con.sql('show tables').show()
