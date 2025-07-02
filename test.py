import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_password = os.getenv("DUCKLAKE_DB_PASSWORD")
db_user = os.getenv("DUCKLAKE_USER")
db_host = os.getenv("DUCKLAKE_HOST")


# directly connect to catalog database
with duckdb.connect() as con:
    con.execute('INSTALL ducklake')
    con.execute('INSTALL postgres')
    con.execute(
        f"ATTACH 'dbname=ducklake_catalog user={db_user} host={db_host} password={db_password}' AS pgdb (TYPE postgres, READ_ONLY);"
    )
    con.execute(f"USE pgdb")
    con.sql('pragma show_tables').show()


# test s3 key
with duckdb.connect() as con:
    con.execute('INSTALL ducklake')
    con.execute('INSTALL postgres')
    con.execute(
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
    con.sql("SELECT * FROM read_csv('s3://duckdb-ci-dashboard-lake/test.csv')").show()


# connect to duckdlake
with duckdb.connect() as con:
    con.execute('INSTALL ducklake')
    con.execute('INSTALL postgres')
    con.execute(
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
    con.execute(
        f"""
        ATTACH 'ducklake:postgres:dbname=ducklake_catalog
            password={os.getenv("DUCKLAKE_DB_PASSWORD")}
            host={os.getenv("DUCKLAKE_HOST")}
            user={os.getenv("DUCKLAKE_USER")}'
        AS my_ducklake
        (DATA_PATH 's3://duckdb-ci-dashboard-lake/');
        """
    )
    con.execute("USE my_ducklake")
    con.sql('show tables').show()
    con.sql('select count(*) from ci_runs').show()
