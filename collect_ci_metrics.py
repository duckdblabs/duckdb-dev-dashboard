import duckdb
import json
import os
import requests
import tempfile
from pathlib import Path

GITHUB_REPO = "duckdb/duckdb"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
DUCKDB_FILE = Path("ci_metrics.duckdb")
TABLE_NAME = "ci_runs"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


def get_latest_previously_stored():
    with duckdb.connect(DUCKDB_FILE) as con:
        if con.sql(f"select 1 from duckdb_tables() where table_name = '{TABLE_NAME}'").fetchone():
            max_id = con.sql(f"SELECT max(id) FROM {TABLE_NAME}").fetchone()[0]
            if max_id != None:
                print(f"latest run previously stored: {max_id}")
                return max_id
            else:
                con.execute(f"drop table {TABLE_NAME}")
    print(f"initial run")
    return 0


def fetch_github_actions_runs(latest_previously_stored, INITIAL_RUN):
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    page = 1
    fetched_workflow_runs = []
    while True:
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
        if INITIAL_RUN and page >= 10:
            break
        page += 1
    print(f"fetched {len(fetched_workflow_runs)} from {GITHUB_API_URL}")
    return fetched_workflow_runs


def store_runs(runs, initial_run, latest_previously_stored):
    runs_str = f"[{',\n'.join([json.dumps(r) for r in runs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(runs_str)
        tmp.flush()
        # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
        subquery = f"""
                    (select * from read_json('{tmp.name}')
                    where id < (select min(id) from read_json('{tmp.name}') where status != 'completed')
                    and id > {latest_previously_stored})
                    """
        with duckdb.connect(DUCKDB_FILE) as con:
            if initial_run:
                con.execute(f"create table ci_runs as {subquery}")
            else:
                con.execute(f"insert into ci_runs {subquery}")
            print('stored rows:')
            con.sql(f"select id, created_at, html_url, '...' as 'more ...' from {subquery} order by id").show()


def main():
    latest_previously_stored = get_latest_previously_stored()
    initial_run = True if latest_previously_stored == 0 else False
    runs = fetch_github_actions_runs(latest_previously_stored, initial_run)
    store_runs(runs, initial_run, latest_previously_stored)


if __name__ == "__main__":
    main()
