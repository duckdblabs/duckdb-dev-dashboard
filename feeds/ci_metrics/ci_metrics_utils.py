from datetime import datetime, timedelta

from utils.ducklake import DuckLakeConnection
from utils.github_utils import get_rate_limit, gh_api_request
from .ci_config import *


class RepoRatelimits:
    # repo 'duckdb/duckdb' gets 10% of the rate limit
    # other repos equally share the remainder
    def __init__(self, repo_names):
        nr_other_repos = len(repo_names) - 1 if DUCKDB_REPO in repo_names else len(repo_names)
        self.total = int(get_rate_limit() * GITHUB_RATE_LIMITING_FACTOR)
        if DUCKDB_REPO in repo_names:
            self.duckdb = int(self.total * 0.1) if nr_other_repos > 0 else self.total
        else:
            self.duckdb = 0
        self.non_duckdb = int((self.total - self.duckdb) / nr_other_repos) if nr_other_repos > 0 else 0

    def get_repo_rate_limit(self, github_repo):
        return self.duckdb if github_repo == DUCKDB_REPO else self.non_duckdb


def fetch_github_actions_runs(rate_limit: int, github_repo: str, latest_previously_stored: int | None = None) -> list:
    endpoint = GITHUB_RUNS_ENDPOINT.format(GITHUB_REPO=github_repo)
    page = 1
    fetched_workflow_runs = []
    print(f"fetching from: {endpoint}")
    while True:
        print(f"page: {page}", flush=True)
        if page > rate_limit:
            if latest_previously_stored == None:
                print(f"rate limit ({rate_limit}) hit!")
            else:
                print(
                    f"WARNING: rate limit ({rate_limit}) hit, but connecting run id not found. Storing nothing to prevent gaps"
                )
                fetched_workflow_runs = []
            break
        params = {"per_page": 100, "page": page}
        resp = gh_api_request(endpoint, params=params)
        data = resp.get("workflow_runs", [])
        fetched_workflow_runs.extend(data)
        if (latest_previously_stored != None) and latest_previously_stored in [run['id'] for run in data]:
            break
        if len(data) < 100:
            break
        page += 1
    print(f"fetched {len(fetched_workflow_runs)} runs")
    return fetched_workflow_runs


def get_recent_run_ids_without_jobs(con: DuckLakeConnection) -> dict[str, list]:
    max_age: int | None = GITHUB_RUNS_STALE_DELAY
    stale_timestamp = (datetime.now() - timedelta(days=max_age)).strftime("%Y-%m-%d %H:%M:%S") if max_age else None
    print(f"fetching runs {f"(created after {stale_timestamp})" if stale_timestamp else ''} without jobs ...", flush=True)

    # fetch recent runs without jobs
    con.execute(f"""
    CREATE OR REPLACE TEMPORARY TABLE recent_runs_without_jobs AS
      SELECT runs.repository['id'] repo_id, runs.id run_id
      FROM {GITHUB_RUNS_TABLE} runs
        ANTI JOIN {GITHUB_JOBS_TABLE} jobs ON runs.id = jobs.run_id
      WHERE runs.status='completed'
        {f"and runs.updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
      ORDER BY run_id ASC
    ;
    """)
    # group by repository
    res = con.sql(f"""
    SELECT
      repos.full_name,
      list_sort(list(selected_runs.run_id))
    FROM {GITHUB_REPOS_TABLE} repos
      LEFT JOIN recent_runs_without_jobs selected_runs
      ON selected_runs.repo_id = repos.id
    GROUP BY repos.full_name;
    """
    ).fetchall()
    repo_jobs: dict[str, list] = {tup[0]: (tup[1] if tup[1] != [None] else []) for tup in res}
    return repo_jobs
