import json
import os
import requests
import sys
import tempfile
from pathlib import Path
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.ducklake import DuckLakeConnection
from utils.github_utils import get_rate_limit, fetch_github_record_list

load_dotenv()

GITHUB_RATE_LIMIT = int(get_rate_limit() * 0.8)  # use max 80% of available rate limit
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = "duckdb/duckdb"

GITHUB_WORKFLOWS_ENDPOINT = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows"
GITHUB_RUNS_ENDPOINT = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs"

CI_RUNS_TABLE = "ci_runs"
CI_WORKFLOWS_TABLE = "ci_workflows"


def main():
    update_workflows()
    update_workflow_runs()
    update_jobs()


def update_workflows():
    with DuckLakeConnection() as con:
        if not con.table_exists(CI_WORKFLOWS_TABLE):
            is_inital_run = True
        else:
            if con.table_empty(CI_WORKFLOWS_TABLE):
                raise ValueError(f"Invalid state - Table {CI_WORKFLOWS_TABLE} should not be empty")
            is_inital_run = False
    total_count, workflows = fetch_github_record_list(GITHUB_WORKFLOWS_ENDPOINT, 'workflows')
    store_workflows(workflows, is_inital_run)


def update_workflow_runs():
    with DuckLakeConnection() as con:
        if not con.table_exists(CI_RUNS_TABLE):
            is_inital_run = True
            latest_previously_stored = 0
            runs = fetch_github_actions_runs(True)
        else:
            if con.table_empty(CI_RUNS_TABLE):
                raise ValueError(f"Invalid state - Table {CI_RUNS_TABLE} should not be empty")
            is_inital_run = False
            latest_previously_stored = con.max_id(CI_RUNS_TABLE)
    runs = fetch_github_actions_runs(is_inital_run, latest_previously_stored)
    store_runs(runs, is_inital_run, latest_previously_stored)


def update_jobs():
    pass


def store_workflows(workflows, is_inital_run):
    workflows_str = f"[{',\n'.join([json.dumps(w) for w in workflows])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(workflows_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
            if is_inital_run:
                subquery = f"(select * from read_json('{tmp.name}'))"
                con.execute(f"create table {CI_WORKFLOWS_TABLE} as {subquery}")
            else:
                subquery = f"""
                            (
                            select * from read_json('{tmp.name}')
                            where id not in (select id from {CI_WORKFLOWS_TABLE})
                            )
                            """
                con.execute(f"insert into {CI_WORKFLOWS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select * from {subquery} order by id").show()


def store_runs(runs, initial_run, latest_previously_stored):
    runs_str = f"[{',\n'.join([json.dumps(r) for r in runs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(runs_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
            subquery = f"""
                        (
                        select * from read_json('{tmp.name}')
                        where id < (select min(id) from read_json('{tmp.name}') where status != 'completed')
                        and id > {latest_previously_stored}
                        )
                        """
            if initial_run:
                con.execute(f"create table {CI_RUNS_TABLE} as {subquery}")
            else:
                con.execute(f"insert into {CI_RUNS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select id, created_at, html_url, '...' as 'more ...' from {subquery} order by id").show()


def fetch_github_actions_runs(initial_run, latest_previously_stored=0):
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    page = 1
    fetched_workflow_runs = []
    print(f"fetching github workflow runs from: {GITHUB_RUNS_ENDPOINT}")
    while True:
        print(f"page: {page}")
        params = {"per_page": 100, "page": page}
        resp = requests.get(GITHUB_RUNS_ENDPOINT, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"GitHub API error: {resp.status_code} {resp.text}")
            exit(1)
        data = resp.json().get("workflow_runs", [])
        fetched_workflow_runs.extend(data)
        if latest_previously_stored in [run['id'] for run in data]:
            break
        if len(data) < 100:
            break
        if initial_run and page >= GITHUB_RATE_LIMIT:
            break
        page += 1
    print(f"fetched {len(fetched_workflow_runs)} runs")
    return fetched_workflow_runs


if __name__ == "__main__":
    main()
