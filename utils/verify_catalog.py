"""
connect with postgres
if the ducklake catalog database is not present, it is created (empty)
"""

from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

CATALOG_DB_NAME = 'ducklake_catalog'


def create_catalog_db_if_not_exists():
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


if __name__ == "__main__":
    create_catalog_db_if_not_exists()
