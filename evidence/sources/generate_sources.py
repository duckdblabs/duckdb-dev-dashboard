"""
This script creates .duckdb persistent files by copying tables from the ducklake.
This is necessary since evidence can not directly use ducklake yet, since it is on an old version of duckdb-wasm

A source can name the lake it comes from ('lake_secret', default: this repo's own lake) and whether
that lake must be attached read-only. Sources are grouped per lake, so each lake is attached once.

Besides copying whole tables ('tables'), a source can materialize the result of a SQL file that is
run against the lake ('derived_tables'). That is for data which is far too large to ship to the
browser raw and only interesting aggregated, e.g. the benchmark results lake, whose query_metrics table
holds one row per query per warm run per metric.

run this file via Makefile: 'make generate_sources'
to refresh a subset (e.g. without having credentials for every lake):
    python3 -m evidence.sources.generate_sources ci_metrics extension_downloads
"""

import json
import sys
from pathlib import Path
from utils.ducklake import DuckLakeConnection

DEFAULT_LAKE_SECRET = 'ducklake_secret'


def generate_source(con: DuckLakeConnection, source: dict):
    print(f"---\ngenerating sources for data-feed: {source['name']} ...")
    con.execute(f"ATTACH '{source['db_path']}' AS {source['name']}")
    try:
        for table in source.get("tables", []):
            if con.table_exists(table):
                con.execute(f"CREATE OR REPLACE TABLE {source['name']}.main.{table} AS FROM {table};")
                print(f"Refreshed file '{source['db_path']}', table: {table} by copying from ducklake", flush=True)
            else:
                print(f"Error: table {table} not present in ducklake; can not refresh: {source['db_path']}!")
        for derived in source.get("derived_tables", []):
            sql_file = Path(derived["sql_file"])
            if not sql_file.is_file():
                print(f"Error: sql file not found: {sql_file}; can not refresh: {source['db_path']}!")
                continue
            # the file's SQL is appended verbatim: a leading comment, a WITH clause and a trailing
            # semicolon are all valid after CREATE TABLE ... AS
            con.execute(
                f"CREATE OR REPLACE TABLE {source['name']}.main.{derived['name']} AS\n{sql_file.read_text()}"
            )
            print(f"Refreshed file '{source['db_path']}', table: {derived['name']} by running {sql_file}", flush=True)
    finally:
        con.execute(f"DETACH {source['name']}")


def main():
    # get config (e.g. which tables apply for which source)
    sources_config = Path("./evidence/sources/sources.json")
    if not sources_config.is_file():
        print(f"Error: source config file not found at {sources_config}")
        sys.exit(1)
    sources = json.loads(sources_config.read_text())

    # optional positional args: only refresh these sources
    only = set(sys.argv[1:])
    if only:
        unknown = only - {source["name"] for source in sources}
        if unknown:
            print(f"Error: unknown source(s): {', '.join(sorted(unknown))}")
            sys.exit(1)
        sources = [source for source in sources if source["name"] in only]

    # group sources per lake, so each lake is attached exactly once
    lakes: dict[tuple[str, bool], list[dict]] = {}
    for source in sources:
        lake = (source.get("lake_secret", DEFAULT_LAKE_SECRET), bool(source.get("read_only", False)))
        lakes.setdefault(lake, []).append(source)

    # create .duckdb source files from ducklake
    for (lake_secret, read_only), lake_sources in lakes.items():
        with DuckLakeConnection(lake_secret, read_only=read_only) as con:
            con.execute(f"SET preserve_insertion_order=false")
            con.execute(f"SET memory_limit = '8GB'")
            for source in lake_sources:
                generate_source(con, source)


if __name__ == "__main__":
    main()
