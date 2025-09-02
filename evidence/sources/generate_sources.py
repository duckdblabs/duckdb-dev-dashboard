"""
This script creates .duckdb persistent files by copying tables from the ducklake.
This is necessary since evidence can not directly use ducklake yet, since it is on an old version of duckdb-wasm

run this file via Makefile: 'make generate_sources'
"""

import json
import sys
from pathlib import Path
from utils.ducklake import DuckLakeConnection


def main():
    # get config (e.g. which tables apply for which source)
    sources_config = Path("./evidence/sources/sources.json")
    if not sources_config.is_file():
        print(f"Error: source config file not found at {sources_config}")
        sys.exit(1)
    sources = json.loads(sources_config.read_text())

    # create .duckdb source files from ducklake
    with DuckLakeConnection() as con:
        for source in sources:
            con.execute(f"ATTACH '{source['db_path']}' AS {source['name']};")
            for table in source["tables"]:
                if con.table_exists(table):
                    con.execute(f"CREATE OR REPLACE TABLE {source['name']}.main.{table} AS FROM {table};")
                    print(f"Refreshed file '{source['db_path']}', table: {table} by copying from ducklake", flush=True)
                else:
                    print(f"Error: table {table} not present in ducklake; can not refresh: {source['db_path']}!")


if __name__ == "__main__":
    main()
