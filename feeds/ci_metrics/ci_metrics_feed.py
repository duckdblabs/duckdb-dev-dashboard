import json
import os
import requests
import tempfile
from datetime import datetime, timedelta
from dotenv import load_dotenv

from utils.ducklake import DuckLakeConnection
from utils.github_utils import get_rate_limit, fetch_github_record_list

load_dotenv()

GITHUB_RATE_LIMITING_FACTOR = 0.8  # use max 80% of available rate limit
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = "duckdb/duckdb"

GITHUB_WORKFLOWS_ENDPOINT = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows"
GITHUB_RUNS_ENDPOINT = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
GITHUB_JOBS_ENDPOINT = "https://api.github.com/repos/{GITHUB_REPO}/actions/runs/{RUN_ID}/jobs"

GITHUB_WORKFLOWS_TABLE = "ci_workflows"
GITHUB_RUNS_TABLE = "ci_runs"
GITHUB_JOBS_TABLE = "ci_jobs"


def run():
    update_workflows()
    update_workflow_runs()
    update_run_jobs()


def update_workflows():
    rate_limit = int(get_rate_limit() * GITHUB_RATE_LIMITING_FACTOR)
    with DuckLakeConnection() as con:
        if not con.table_exists(GITHUB_WORKFLOWS_TABLE):
            is_inital_run = True
        else:
            if con.table_empty(GITHUB_WORKFLOWS_TABLE):
                raise ValueError(f"Invalid state - Table {GITHUB_WORKFLOWS_TABLE} should not be empty")
            is_inital_run = False
    _, workflows, error = fetch_github_record_list(GITHUB_WORKFLOWS_ENDPOINT, 'workflows', rate_limit, detail_log=True)
    if error:
        print(error)
    else:
        store_workflows(workflows, is_inital_run)


def update_workflow_runs():
    rate_limit = int(get_rate_limit() * GITHUB_RATE_LIMITING_FACTOR)
    with DuckLakeConnection() as con:
        if not con.table_exists(GITHUB_RUNS_TABLE):
            is_inital_run = True
            latest_previously_stored = 0
        else:
            if con.table_empty(GITHUB_RUNS_TABLE):
                raise ValueError(f"Invalid state - Table {GITHUB_RUNS_TABLE} should not be empty")
            is_inital_run = False
            latest_previously_stored = con.max_id(GITHUB_RUNS_TABLE)
    runs = fetch_github_actions_runs(is_inital_run, rate_limit, latest_previously_stored)
    if runs:
        store_runs(runs, is_inital_run, latest_previously_stored)


def update_run_jobs():
    adjusted_rate_limit = int(get_rate_limit() * GITHUB_RATE_LIMITING_FACTOR)
    # first get the run_ids, we need them to fetch the jobs
    with DuckLakeConnection() as con:
        assert con.table_exists(GITHUB_RUNS_TABLE)
        is_inital_run = True if not con.table_exists(GITHUB_JOBS_TABLE) else False
        if is_inital_run:
            run_ids = con.sql(
                f"select id from {GITHUB_RUNS_TABLE} where status='completed' order by id ASC limit {adjusted_rate_limit}"
            ).fetchall()
        else:
            # Runs without jobs are considered 'stale' after 48 hours.
            # We'll stop querying for jobs for stale runs, to prevent repeating useless API calls.
            # To fetch the full history, replace with: stale_timestamp = None
            stale_timestamp = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
            # fetch the runs for which the jobs are still missing
            total_count_star = con.sql(
                f"""
                SELECT count(*)
                FROM ci_runs runs
                LEFT JOIN ci_jobs jobs ON runs.id = jobs.run_id
                WHERE jobs.run_id is NULL
                {f"AND runs.updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
                """
            ).fetchone()[0]
            print(f"jobs need to be fetched for {total_count_star} runs")
            if total_count_star > adjusted_rate_limit:
                print(f"applying rate limit: fetching jobs for {adjusted_rate_limit} runs")
            run_ids = con.sql(
                f"""
                SELECT runs.id
                FROM ci_runs runs
                LEFT JOIN ci_jobs jobs ON runs.id = jobs.run_id
                WHERE jobs.run_id is NULL
                {f"AND runs.updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
                ORDER BY runs.id ASC
                LIMIT {adjusted_rate_limit}
            """
            ).fetchall()
    # fetch jobs from github
    new_jobs = []
    print('fetching jobs per run:')
    total_runs = len(run_ids)
    count = 1
    for (run_id,) in run_ids:
        print(f"{count}/{total_runs}", flush=True)
        endpoint = GITHUB_JOBS_ENDPOINT.format(GITHUB_REPO=GITHUB_REPO, RUN_ID=run_id)
        _, jobs, error = fetch_github_record_list(endpoint, 'jobs', adjusted_rate_limit, detail_log=True)
        if error:
            print(error)
        else:
            new_jobs.extend(jobs)
        count += 1
    # store in ducklake
    if new_jobs:
        store_run_jobs(new_jobs, is_inital_run)


def store_run_jobs(jobs, is_inital_run):
    jobs_str = f"[{',\n'.join([json.dumps(j) for j in jobs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(jobs_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            subquery = f"(select * from read_json('{tmp.name}'))"
            if is_inital_run:
                con.execute(f"create table {GITHUB_JOBS_TABLE} as {subquery}")
            else:
                con.execute(f"insert into {GITHUB_JOBS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select * from {subquery} order by id").show()


def store_workflows(workflows, is_inital_run):
    # TODO: use 'upsert' (currently, we only append workflows)
    workflows_str = f"[{',\n'.join([json.dumps(w) for w in workflows])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(workflows_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
            if is_inital_run:
                subquery = f"(select * from read_json('{tmp.name}'))"
                con.execute(f"create table {GITHUB_WORKFLOWS_TABLE} as {subquery}")
            else:
                subquery = f"""
                            (
                            select * from read_json('{tmp.name}')
                            where id not in (select id from {GITHUB_WORKFLOWS_TABLE})
                            )
                            """
                con.execute(f"insert into {GITHUB_WORKFLOWS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select * from {subquery} order by id").show()


def store_runs(runs, is_initial_run, latest_previously_stored):
    runs_str = f"[{',\n'.join([json.dumps(r) for r in runs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(runs_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
            # runs are considered 'stale' after 48 hours (even if their status somehow is stuck in 'in progress')
            stale_timestamp = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
            subquery = f"""
                        (
                        select * from read_json('{tmp.name}')
                        where id < (select min(id) from read_json('{tmp.name}') where status != 'completed' and updated_at > TIMESTAMP '{stale_timestamp}')
                        and id > {latest_previously_stored}
                        )
                        """
            if is_initial_run:
                con.execute(f"create table {GITHUB_RUNS_TABLE} as {subquery}")
            else:
                con.execute(f"insert into {GITHUB_RUNS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select id, created_at, status, html_url, '...' as 'more ...' from {subquery} order by id").show()


def fetch_github_actions_runs(initial_run, rate_limit, latest_previously_stored=0):
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    page = 1
    fetched_workflow_runs = []
    print(f"fetching from: {GITHUB_RUNS_ENDPOINT}")
    while True:
        print(f"page: {page}", flush=True)
        params = {"per_page": 100, "page": page}
        resp = requests.get(GITHUB_RUNS_ENDPOINT, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"fetching from: {GITHUB_RUNS_ENDPOINT}")
            print(f"GitHub API error: {resp.status_code} {resp.text}")
            exit(1)
        data = resp.json().get("workflow_runs", [])
        fetched_workflow_runs.extend(data)
        if latest_previously_stored in [run['id'] for run in data]:
            break
        if len(data) < 100:
            break
        if initial_run and page >= rate_limit:
            break
        page += 1
    print(f"fetched {len(fetched_workflow_runs)} runs")
    return fetched_workflow_runs


if __name__ == "__main__":
    run()
