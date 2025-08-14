from collections import OrderedDict
import duckdb
from dotenv import load_dotenv
import json
import os
import tempfile
import psycopg2


class DuckLakeConnection:
    def __init__(self):
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
        self.con = None
        self.catalog_name = 'ducklake_catalog'
        self._create_catalog_db_if_not_exists()

    def __enter__(self):
        self.con = duckdb.connect()
        self.con.execute('INSTALL ducklake; LOAD ducklake;')
        self.con.execute('INSTALL postgres; LOAD postgres;')
        self.con.execute(
            f"""
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{os.getenv("S3_KEY_ID")}',
                SECRET '{os.getenv("S3_SECRET_KEY")}'
            )
            """
        )
        self.con.execute(
            f"""
            ATTACH 'ducklake:postgres:dbname={self.catalog_name}
                password={os.getenv("DUCKLAKE_DB_PASSWORD")}
                host={os.getenv("DUCKLAKE_HOST")}
                user={os.getenv("DUCKLAKE_USER")}'
            AS my_ducklake
            (DATA_PATH '{os.getenv("S3_ENDPOINT")}');
            """
        )
        self.con.execute("USE my_ducklake")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.con.close()

    def _create_catalog_db_if_not_exists(self):
        # create postgres catalog db (empty)
        CATALOG_DB_NAME = self.catalog_name
        con = psycopg2.connect(
            dbname="postgres",
            user=os.environ["DUCKLAKE_USER"],
            host=os.environ["DUCKLAKE_HOST"],
            password=os.environ["DUCKLAKE_DB_PASSWORD"]
        )
        con.autocommit = True
        try:
            with con.cursor() as cursor:
                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{CATALOG_DB_NAME}'")
                if cursor.fetchone():
                    print(f"Ducklake catalog database found.")
                else:
                    cursor.execute(f"CREATE DATABASE {CATALOG_DB_NAME}")
                    print(f"Ducklake catalog database created.")
        finally:
            con.close()

    def sql(self, sql_str):
        return self.con.sql(sql_str)

    def execute(self, sql_str):
        return self.con.execute(sql_str)

    def table_exists(self, table_name: str) -> bool:
        return self.con.sql(f"select 1 from duckdb_tables() where table_name='{table_name}'").fetchone() == (1,)

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


# # example usage:
# with DuckLakeConnection() as con:
#     con.sql('show tables').show()
