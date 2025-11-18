GITHUB_ORG = "duckdb"
DUCKDB_REPO = "duckdb/duckdb"

# ducklake table names
GITHUB_REPOS_TABLE = "ci_repositories"
GITHUB_WORKFLOWS_TABLE = "ci_workflows"
GITHUB_RUNS_TABLE = "ci_runs"
GITHUB_JOBS_TABLE = "ci_jobs"

# github endpoints
GITHUB_REPOS_ENDPOINT = "https://api.github.com/orgs/{GITHUB_ORG}/repos".format(GITHUB_ORG=GITHUB_ORG)
GITHUB_WORKFLOWS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/workflows"
GITHUB_RUNS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
GITHUB_JOBS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/runs/{RUN_ID}/jobs"

GITHUB_RATE_LIMITING_FACTOR = 0.80  # use max 80% of available rate limit

# number of HOURS after which CI runs are considered stale
# (e.g. if they didn't get to status=completed by now, they probably never will)
GITHUB_RUNS_STALE_DELAY: int | None = 48

# after this number of DAYS, we stop trying to fetch jobs for this run
GITHUB_RUNS_JOB_CUTOFF: int | None = None
