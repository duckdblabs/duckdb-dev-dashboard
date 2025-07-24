import duckdb
import os
from dotenv import load_dotenv


class DuckLakeConnection:
    def __init__(self):
        load_dotenv()
        for env_var in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "DUCKLAKE_DB_PASSWORD",
            "DUCKLAKE_HOST",
            "DUCKLAKE_USER",
        ]:
            if env_var not in os.environ.keys():
                raise ValueError(f"Env variable '{env_var}' is missing!")
        self.con = None

    def __enter__(self):
        self.con = duckdb.connect()
        self.con.execute('INSTALL ducklake; LOAD ducklake;')
        self.con.execute('INSTALL postgres; LOAD postgres;')
        self.con.execute(
            f"""
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{os.getenv("AWS_ACCESS_KEY_ID")}',
                SECRET '{os.getenv("AWS_SECRET_ACCESS_KEY")}',
                REGION '{os.getenv("AWS_REGION")}'
            )
            """
        )
        self.con.execute(
            f"""
            ATTACH 'ducklake:postgres:dbname=ducklake_catalog
                password={os.getenv("DUCKLAKE_DB_PASSWORD")}
                host={os.getenv("DUCKLAKE_HOST")}
                user={os.getenv("DUCKLAKE_USER")}'
            AS my_ducklake
            (DATA_PATH 's3://duckdb-ci-dashboard-lake/');
            """
        )
        self.con.execute("USE my_ducklake")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.con.close()

    def sql(self, sql_str):
        return self.con.sql(sql_str)

    def execute(self, sql_str):
        return self.con.execute(sql_str)

    def table_exists(self, table_name: str) -> bool:
        return self.con.sql(f"select 1 from duckdb_tables() where table_name='{table_name}'").fetchone() == (1,)

    def table_empty(self, table_name: str) -> bool:
        return self.con.sql(f"select count(*) from {table_name}").fetchone() == (0,)

    def max_id(self, table_name: str) -> int | None:
        return self.con.sql(f"select max(id) from {table_name}").fetchone()[0]
