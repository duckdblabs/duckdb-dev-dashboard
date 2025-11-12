import json
import tempfile
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

from utils.ducklake import DuckLakeConnection
from utils.github_utils import get_rate_limit, fetch_github_record_list, fetch_repo_names, gh_api_request

load_dotenv()

GITHUB_RATE_limitING_FACTOR = 0.80  # use max 80% of available rate limit
GITHUB_ORG = "duckdb"

GITHUB_WORKFLOWS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/workflows"
GITHUB_RUNS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
GITHUB_JOBS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/runs/{RUN_ID}/jobs"

GITHUB_WORKFLOWS_TABLE = "ci_workflows"
GITHUB_RUNS_TABLE = "ci_runs"
GITHUB_JOBS_TABLE = "ci_jobs"

class RepoRatelimits:
    DUCKDB_REPO = "duckdb/duckdb"

    # repo 'duckdb/duckdb' gets 50% of the rate limit
    # other repos equally share the remainder
    def __init__(self, repo_names):
        nr_other_repos = len(repo_names) - 1 if RepoRatelimits.DUCKDB_REPO in repo_names else len(repo_names)
        self.total = int(get_rate_limit() * GITHUB_RATE_limitING_FACTOR)
        if RepoRatelimits.DUCKDB_REPO in repo_names:
            self.duckdb = int(self.total * 0.5) if nr_other_repos > 0 else self.total
        else:
            self.duckdb = 0
        self.non_duckdb = int((self.total - self.duckdb) / nr_other_repos) if nr_other_repos > 0 else 0

    def get_repo_rate_limit(self, github_repo):
        return self.duckdb if github_repo == RepoRatelimits.DUCKDB_REPO else self.non_duckdb


def run():
    repo_names = fetch_repo_names(GITHUB_ORG)
    if not repo_names:
        raise ValueError(f"No repositories could be fetched for organisation '{GITHUB_ORG}'")

    for repo_name in repo_names:
        update_workflows(repo_name)

    rate_limit_runs = RepoRatelimits(repo_names)
    for repo_name in repo_names:
        update_runs(repo_name, rate_limit_runs)

    rate_limit_jobs = RepoRatelimits(repo_names)
    for repo_name in repo_names:
        update_jobs(repo_name, rate_limit_jobs)


def update_workflows(github_repo):
    print(f"updating workflows for: {github_repo}")
    assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", github_repo), "regex not matched" # format: 'org/repo_name'
    endpoint = GITHUB_WORKFLOWS_ENDPOINT.format(GITHUB_REPO=github_repo)
    with DuckLakeConnection() as con:
        if not con.table_exists(GITHUB_WORKFLOWS_TABLE):
            create_table = True
        else:
            if con.table_empty(GITHUB_WORKFLOWS_TABLE):
                raise ValueError(f"Invalid state - Table {GITHUB_WORKFLOWS_TABLE} should not be empty")
            create_table = False
    _, workflows = fetch_github_record_list(endpoint, 'workflows', detail_log=True)
    if workflows:
        for workflow in workflows:
            workflow['repository'] = github_repo
        store_workflows(workflows, create_table)
    else:
        print(f"no workflows found")


def update_runs(github_repo: str, rate_limits: RepoRatelimits):
    print(f"updating workflow runs for: {github_repo}")
    assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", github_repo), "regex not matched" # format: 'org/repo_name'
    rate_limit = rate_limits.get_repo_rate_limit(github_repo)
    with DuckLakeConnection() as con:
        if not con.table_exists(GITHUB_RUNS_TABLE):
            create_table = True
            latest_previously_stored = None
        else:
            if con.table_empty(GITHUB_RUNS_TABLE):
                raise ValueError(f"Invalid state - Table {GITHUB_RUNS_TABLE} should not be empty")
            create_table = False
            con.execute(f"select max(id) from {GITHUB_RUNS_TABLE} where repository['full_name'] = ?", [github_repo])
            latest_previously_stored = con.fetchone()[0]
    runs = fetch_github_actions_runs(rate_limit, github_repo, latest_previously_stored)
    if runs:
        store_runs(runs, create_table, latest_previously_stored)


def update_jobs(github_repo, rate_limits: RepoRatelimits):
    print(f"updating jobs for: {github_repo}")
    assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", github_repo), "regex not matched" # format: 'org/repo_name'
    rate_limit = rate_limits.get_repo_rate_limit(github_repo)

    # first get the run_ids, we need them to fetch the jobs
    with DuckLakeConnection() as con:
        assert con.table_exists(GITHUB_RUNS_TABLE), f"tabel {GITHUB_RUNS_TABLE} does not exist"
        create_table = True if not con.table_exists(GITHUB_JOBS_TABLE) else False
        if create_table:
            con.execute(
                f"""
                select runs.id
                from {GITHUB_RUNS_TABLE} runs
                where runs.status='completed'
                and runs.repository['full_name'] = ?
                order by runs.id ASC
                limit {rate_limit}
                """,
                [github_repo]
            )
            run_ids = con.fetchall()
        else:
            # Runs without jobs are considered 'stale' after 48 hours.
            # We'll stop querying for jobs for stale runs, to prevent repeating useless API calls.
            # To fetch the full history, replace with: stale_timestamp = None
            stale_timestamp = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
            # fetch the runs for which the jobs are still missing
            con.execute(
                f"""
                select count(*)
                from {GITHUB_RUNS_TABLE} runs
                left join {GITHUB_JOBS_TABLE} jobs on runs.id = jobs.run_id
                where jobs.run_id is NULL
                {f"and runs.updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
                and runs.repository['full_name'] = ?
                """,
                [github_repo]
            )
            count_runs_without_jobs = con.fetchone()[0]
            print(f"jobs need to be fetched for {count_runs_without_jobs} runs for repo {github_repo}")
            if count_runs_without_jobs > rate_limit:
                print(f"applying rate limit: fetching jobs for {rate_limit} runs")
            con.execute(
                f"""
                select runs.id
                from {GITHUB_RUNS_TABLE} runs
                left join {GITHUB_JOBS_TABLE} jobs on runs.id = jobs.run_id
                where jobs.run_id is NULL
                {f"and runs.updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
                and runs.repository['full_name'] = ?
                order by runs.id ASC
                limit {rate_limit}
                """,
                [github_repo]
            )
            run_ids = con.fetchall()

    # fetch jobs from github
    new_jobs = []
    print('fetching jobs per run:')
    total_runs = len(run_ids)
    count = 1
    for (run_id,) in run_ids:
        print(f"{count}/{total_runs}", flush=True)
        endpoint = GITHUB_JOBS_ENDPOINT.format(GITHUB_REPO=github_repo, RUN_ID=run_id)
        _, jobs = fetch_github_record_list(endpoint, 'jobs', rate_limit, detail_log=True)
        new_jobs.extend(jobs)
        count += 1
    # store in ducklake
    if new_jobs:
        store_jobs(new_jobs, create_table)


def store_workflows(workflows, create_table):
    workflows_str = f"[{',\n'.join([json.dumps(w) for w in workflows])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(workflows_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            subquery = f"(select * from read_json('{tmp.name}'))"
            if create_table:
                con.execute(f"create table {GITHUB_WORKFLOWS_TABLE} as {subquery}")
                print('created table and stored rows:')
                con.sql(f"select * from {GITHUB_WORKFLOWS_TABLE} order by id").show()
            else:
                con.execute(f"""
                            merge into {GITHUB_WORKFLOWS_TABLE}
                              using {subquery} as upserts
                              on ({GITHUB_WORKFLOWS_TABLE}.id = upserts.id and {GITHUB_WORKFLOWS_TABLE}.repository = upserts.repository)
                              when matched then update
                              when not matched then insert;
                            """)
                print("new or updated workflows:")
                current_snapshot = con.current_snapshot()
                con.table_changes(GITHUB_WORKFLOWS_TABLE, current_snapshot, current_snapshot).show()


def store_runs(runs, create_table, latest_previously_stored):
    runs_str = f"[{',\n'.join([json.dumps(r) for r in runs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(runs_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
            # runs are considered 'stale' after 48 hours (even if their status somehow is stuck in 'in progress')
            stale_timestamp = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
            oldest_non_completed = con.sql(f"select min(id) from read_json('{tmp.name}') where status != 'completed' and updated_at > TIMESTAMP '{stale_timestamp}'").fetchone()[0]
            subquery = f"""
                        (
                        select * from read_json('{tmp.name}')
                        where True
                        {f"and id < {oldest_non_completed}" if oldest_non_completed else ''}
                        {f"and id > {latest_previously_stored}" if latest_previously_stored else ''}
                        )
                        """
            if create_table:
                con.execute(f"create table {GITHUB_RUNS_TABLE} as {subquery}")
            else:
                con.execute(f"insert into {GITHUB_RUNS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select id, created_at, status, html_url, '...' as 'more ...' from {subquery} order by id").show()


def store_jobs(jobs, create_table):
    jobs_str = f"[{',\n'.join([json.dumps(j) for j in jobs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(jobs_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            subquery = f"(select * from read_json('{tmp.name}'))"
            if create_table:
                con.execute(f"create table {GITHUB_JOBS_TABLE} as {subquery}")
            else:
                con.execute(f"insert into {GITHUB_JOBS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select * from {subquery} order by id").show()


def fetch_github_actions_runs(rate_limit: int, github_repo: str, latest_previously_stored: int | None = None ) -> list:
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
                print(f"WARNING: rate limit ({rate_limit}) hit, but connecting run id not found. Storing nothing to prevent gaps")
                fetched_workflow_runs = []
            break
        if len(data) < 100:
            break
        page += 1
    print(f"fetched {len(fetched_workflow_runs)} runs")
    return fetched_workflow_runs


if __name__ == "__main__":
    run()
