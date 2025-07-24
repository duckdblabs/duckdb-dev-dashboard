import json
import os
import requests
import sys
import tempfile
from pathlib import Path
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.ducklake import DuckLakeConnection

load_dotenv()

GITHUB_REPO = "duckdb/duckdb"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
CI_RUNS_TABLE = "ci_runs"


def main():
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
    store_runs(runs, False, latest_previously_stored)


def fetch_github_actions_runs(initial_run, latest_previously_stored=0):
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    page = 1
    fetched_workflow_runs = []
    print(f"fetching github workflow runs from: {GITHUB_API_URL}")
    while True:
        print(f"page: {page}")
        params = {"per_page": 100, "page": page}
        resp = requests.get(GITHUB_API_URL, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"GitHub API error: {resp.status_code} {resp.text}")
            exit(1)
        data = resp.json().get("workflow_runs", [])
        fetched_workflow_runs.extend(data)
        if latest_previously_stored in [run['id'] for run in data]:
            break
        if len(data) < 100:
            break
        if initial_run and page >= 40:
            break
        page += 1
    print(f"fetched {len(fetched_workflow_runs)} runs")
    return fetched_workflow_runs


def store_runs(runs, initial_run, latest_previously_stored):
    runs_str = f"[{',\n'.join([json.dumps(r) for r in runs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(runs_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
            subquery = f"""
                        (select * from read_json('{tmp.name}')
                        where id < (select min(id) from read_json('{tmp.name}') where status != 'completed')
                        and id > {latest_previously_stored})
                        """
            if initial_run:
                con.execute(f"create table {CI_RUNS_TABLE} as {subquery}")
            else:
                con.execute(f"insert into {CI_RUNS_TABLE} {subquery}")
            print('stored rows:')
            con.sql(f"select id, created_at, html_url, '...' as 'more ...' from {subquery} order by id").show()


if __name__ == "__main__":
    main()
