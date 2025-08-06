# run this file via Makefile: 'make run_feeds'

import feeds.ci_metrics.ci_metrics_feed as ci_metrics_feed
import feeds.extension_downloads.extension_downloads_feed as extension_downloads_feed


def run_all_feeds():
    ci_metrics_feed.run()
    extension_downloads_feed.run()


if __name__ == "__main__":
    run_all_feeds()
