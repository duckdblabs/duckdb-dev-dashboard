.PHONY: all run_feeds generate_sources build dev

all: run_feeds generate_sources build

# run feeds to update the ducklake
run_feeds:
	python3 run_feeds.py

# generates duckdb files (required by evidence)
generate_sources:
	./generate_sources.sh

# build the front-end
build:
	npm --prefix ./evidence run sources
	npm --prefix ./evidence run build

# locally test the front end
dev:
	npm --prefix ./evidence run dev
