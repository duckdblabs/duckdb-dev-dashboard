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
    sources_dir = Path("./evidence/sources")
    if not sources_config.is_file():
        print(f"Error: source config file not found at {sources_config}")
        sys.exit(1)
    sources = json.loads(sources_config.read_text())

    # create .duckdb source files from ducklake
    with DuckLakeConnection() as con:
        # create base tables
        for source in sources:
            print(f"---\ngenerating sources for data-feed: {source['name']} ...")
            con.execute(f"ATTACH '{source['db_path']}' AS {source['name']};")
            for table in source["tables"]:
                if con.table_exists(table):
                    con.execute(f"CREATE OR REPLACE TABLE {source['name']}.main.{table} AS FROM {table};")
                    print(f"Refreshed file '{source['db_path']}', table: {table} by copying from ducklake", flush=True)
                else:
                    print(f"Error: table {table} not present in ducklake; can not refresh: {source['db_path']}!")

            if "materialized_views" in source:
                # Materialized views (implemented as regular tables)
                for mview in source["materialized_views"]:
                    query_file: Path = sources_dir / mview["query_file"]
                    if query_file.is_file():
                        mview_name = mview["table_name"]
                        mv_query = query_file.read_text()
                        con.execute(f"CREATE OR REPLACE TABLE {source['name']}.main.{mview_name} AS {mv_query};")
                        print(f"Refreshed mview: '{mview_name}' by running query file: {query_file.name}", flush=True)
                    else:
                        print(f"Error: can not re-create MView: {mview_name}; file {query_file} not found!")


if __name__ == "__main__":
    main()
