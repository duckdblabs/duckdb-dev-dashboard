# run this file via Makefile: 'make run_feeds' or 'make run_feeds_local'

import duckdb
import sys

import feeds.compactor.compactor as compactor
import feeds.ci_metrics.ci_metrics_feed as ci_metrics_feed
import feeds.extension_downloads.extension_downloads_feed as extension_downloads_feed


FEEDS = [
    # ("compactor", compactor.run),
    ("ci_metrics_feed", ci_metrics_feed.run),
    # ("extension_downloads_feed", extension_downloads_feed.run),
]


def run_all_feeds(dl_secret: str):
    for name, run_func in FEEDS:
        print("------------------")
        print(f"running {name} ...", flush=True)
        try:
            run_func(dl_secret)
        except (ValueError) as e:
            print(f"::warning title={name}::data-feed '{name}' failed: {e}")
        except (AssertionError) as e:
            print(f"::warning title={name}::data-feed '{name}' AssertionError: {e}")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        dl_secret = 'ducklake_secret'
    elif len(sys.argv) == 2 and sys.argv[1] == 'local':
        dl_secret = 'ducklake_secret_local'
    else:
        raise ValueError("Invalid arguments for run_feeds()")

    # check if ducklake secret is available
    with duckdb.connect() as con:
        res = con.sql(f"from duckdb_secrets() where name = '{dl_secret}' and type = 'ducklake'").fetchone()
        if not res:
            raise ValueError(f"Connection secret with name '{dl_secret}' is not found")
    run_all_feeds(dl_secret)
