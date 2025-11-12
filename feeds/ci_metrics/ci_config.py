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
