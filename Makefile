.PHONY: all secrets run_feeds generate_sources build dev

all: run_feeds generate_sources build

# create persistent ducklake secrets
secrets:
	python3 -m utils.create_ducklake_secrets

# run feeds to update the ducklake
run_feeds:
	python3 -m utils.verify_catalog
	python3 -m feeds.run_feeds

# generates duckdb files from ducklake (required by evidence)
generate_sources:
	python3 -m evidence.sources.generate_sources

# build the front-end
build:
	npm --prefix ./evidence run sources
	npm --prefix ./evidence run build

# locally test the front end
dev:
	npm --prefix ./evidence run dev

# locally set up python venv
venv:
	python3 -m venv venv
	./venv/bin/python3 -m pip install -r requirements.txt
