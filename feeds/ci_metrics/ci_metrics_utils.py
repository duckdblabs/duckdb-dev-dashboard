from datetime import datetime, timedelta

from utils.ducklake import DuckLakeConnection
from utils.github_utils import get_rate_limit, gh_api_request
from .ci_config import *


class RepoRatelimits:
    # repo 'duckdb/duckdb' gets 50% of the rate limit
    # other repos equally share the remainder
    def __init__(self, repo_names):
        nr_other_repos = len(repo_names) - 1 if DUCKDB_REPO in repo_names else len(repo_names)
        self.total = int(get_rate_limit() * GITHUB_RATE_LIMITING_FACTOR)
        if DUCKDB_REPO in repo_names:
            self.duckdb = int(self.total * 0.5) if nr_other_repos > 0 else self.total
        else:
            self.duckdb = 0
        self.non_duckdb = int((self.total - self.duckdb) / nr_other_repos) if nr_other_repos > 0 else 0

    def get_repo_rate_limit(self, github_repo):
        return self.duckdb if github_repo == DUCKDB_REPO else self.non_duckdb


def get_jobs_table_state(con: DuckLakeConnection, github_repo: str) -> tuple[bool, bool]:
    if not con.table_exists(GITHUB_JOBS_TABLE):
        create_table = True
        is_first_run = True
    else:
        create_table = False
        is_first_run: bool = con.sql(
            f"""
            select not exists (
              select 1
              from {GITHUB_JOBS_TABLE}
              join {GITHUB_RUNS_TABLE} on {GITHUB_JOBS_TABLE}.run_id = {GITHUB_RUNS_TABLE}.id
              where {GITHUB_RUNS_TABLE}.repository['full_name'] == '{github_repo}'
            );
            """
        ).fetchone()[0]
    return (create_table, is_first_run)


def get_run_ids_count(con: DuckLakeConnection, github_repo: str) -> int:
    con.execute(
        f"""
        select count(*)
        from {GITHUB_RUNS_TABLE} runs
        where runs.status='completed'
        and runs.repository['full_name'] = ?
        order by runs.id ASC
        """,
        [github_repo],
    )
    return con.fetchone()[0]


def get_recent_run_ids_without_jobs_count(
    con: DuckLakeConnection, github_repo: str, max_age: int | None = GITHUB_RUNS_STALE_DELAY
) -> int:
    # Note: max_age age can be set to to filter out stale runs.
    stale_timestamp = (datetime.now() - timedelta(days=max_age)).strftime("%Y-%m-%d %H:%M:%S") if max_age else None
    con.execute(
        f"""
        select count(*)
        from {GITHUB_RUNS_TABLE} runs
        left join {GITHUB_JOBS_TABLE} jobs on runs.id = jobs.run_id
        where jobs.run_id is NULL
        and runs.status='completed'
        {f"and runs.updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
        and runs.repository['full_name'] = ?
        """,
        [github_repo],
    )
    return con.fetchone()[0]


def get_run_ids(con: DuckLakeConnection, github_repo: str, limit: int | None = None) -> list[int]:
    con.execute(
        f"""
        select runs.id
        from {GITHUB_RUNS_TABLE} runs
        where runs.status='completed'
        and runs.repository['full_name'] = ?
        order by runs.id ASC
        {f"limit {limit}" if limit else ''}
        """,
        [github_repo],
    )
    return con.fetchall()


# fetch the runs for which the jobs are still missing
def get_recent_run_ids_without_jobs(
    con: DuckLakeConnection, github_repo: str, limit: int | None = None, max_age: int | None = GITHUB_RUNS_STALE_DELAY
) -> list[int]:
    # Note: max_age age can be set to to filter out stale runs.
    stale_timestamp = (datetime.now() - timedelta(days=max_age)).strftime("%Y-%m-%d %H:%M:%S") if max_age else None
    con.execute(
        f"""
        select runs.id
        from {GITHUB_RUNS_TABLE} runs
        left join {GITHUB_JOBS_TABLE} jobs on runs.id = jobs.run_id
        where jobs.run_id is NULL
        and runs.status='completed'
        {f"and runs.updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
        and runs.repository['full_name'] = ?
        order by runs.id ASC
        {f"limit {limit}" if limit else ''}
        """,
        [github_repo],
    )
    return con.fetchall()


def fetch_github_actions_runs(rate_limit: int, github_repo: str, latest_previously_stored: int | None = None) -> list:
    endpoint = GITHUB_RUNS_ENDPOINT.format(GITHUB_REPO=github_repo)
    page = 1
    fetched_workflow_runs = []
    print(f"fetching from: {endpoint}")
    while True:
        print(f"page: {page}", flush=True)
        params = {"per_page": 100, "page": page}
        resp = gh_api_request(endpoint, params=params)
        data = resp.get("workflow_runs", [])
        fetched_workflow_runs.extend(data)
        if (latest_previously_stored != None) and latest_previously_stored in [run['id'] for run in data]:
            break
        if page >= rate_limit:
            if latest_previously_stored == None:
                print(f"rate limit ({rate_limit}) hit!")
            else:
                print(
                    f"WARNING: rate limit ({rate_limit}) hit, but connecting run id not found. Storing nothing to prevent gaps"
                )
                fetched_workflow_runs = []
            break
        if len(data) < 100:
            break
        page += 1
    print(f"fetched {len(fetched_workflow_runs)} runs")
    return fetched_workflow_runs
