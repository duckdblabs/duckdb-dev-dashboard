# run this file via Makefile: 'make run_feeds'

import feeds.ci_metrics.ci_metrics_feed as ci_metrics_feed
import feeds.extension_downloads.extension_downloads_feed as extension_downloads_feed


FEEDS = [
    ("ci_metrics_feed", ci_metrics_feed.run),
    ("extension_downloads_feed", extension_downloads_feed.run),
]


def run_all_feeds():
    for name, run_func in FEEDS:
        print("------------------")
        print(f"running {name} ...", flush=True)
        run_func()


if __name__ == "__main__":
    run_all_feeds()
