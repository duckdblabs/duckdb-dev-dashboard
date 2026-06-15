.PHONY: all secrets run_feeds maintain generate_sources build dev venv sync_local run_feeds_local

all: run_feeds generate_sources build

# create persistent ducklake secrets
secrets:
	python3 -m utils.create_ducklake_secrets

# run feeds to update the ducklake
run_feeds:
	python3 -m feeds.run_feeds

maintain:
	duckdb -f utils/maintenance.sql

# generates duckdb files from ducklake (required by evidence)
generate_sources:
	python3 -m evidence.sources.generate_sources

# build the front-end; this creates a parquet file per table in duckdb file
build:
	npm --prefix ./evidence run sources
	npm --prefix ./evidence run build

# locally test the front end
dev:
	npm --prefix ./evidence run dev

# locally set up python venv
venv:
	python3 -m venv .venv
	./.venv/bin/python3 -m pip install -r requirements.txt

# create a local copy of the ducklake, and secret: 'ducklake_secret_local'
sync_local:
	python3 -m utils.sync_local

run_feeds_local:
	python3 -m feeds.run_feeds local
