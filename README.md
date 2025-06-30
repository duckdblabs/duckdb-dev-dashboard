# CI Dashboard
Script that fetches and stores completed CI runs from: https://api.github.com/repos/duckdb/duckdb/actions/runs

Note that only consecutive 'completed' runs are stored.
After an initial run the script will only add new completed runs ('append only').
