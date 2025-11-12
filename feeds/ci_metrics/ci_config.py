GITHUB_ORG = "duckdb"
DUCKDB_REPO = "duckdb/duckdb"

# ducklake table names
GITHUB_WORKFLOWS_TABLE = "ci_workflows"
GITHUB_RUNS_TABLE = "ci_runs"
GITHUB_JOBS_TABLE = "ci_jobs"

# github endpoints
GITHUB_WORKFLOWS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/workflows"
GITHUB_RUNS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
GITHUB_JOBS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/runs/{RUN_ID}/jobs"

GITHUB_RATE_LIMITING_FACTOR = 0.80  # use max 80% of available rate limit

# number of days after which CI runs are considered stale.
# after this moment, we stop fetching jobs for this run
GITHUB_RUNS_STALE_DELAY: int | None = None
