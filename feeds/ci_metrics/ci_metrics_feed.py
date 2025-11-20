import json
import tempfile
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

from utils.ducklake import DuckLakeConnection
from utils.github_utils import fetch_github_record_list, fetch_repo_names
from .ci_metrics_utils import (
    RepoRatelimits,
    get_jobs_table_state,
    get_run_ids,
    get_run_ids_count,
    get_recent_run_ids_without_jobs,
    get_recent_run_ids_without_jobs_count,
    fetch_github_actions_runs,
)
from .ci_config import *

load_dotenv()


def run():
    repo_names = fetch_repo_names(GITHUB_ORG)
    if not repo_names:
        raise ValueError(f"No repositories could be fetched for organisation '{GITHUB_ORG}'")

    print(f"===============\nupdating ci workflows")
    for repo_name in repo_names:
        update_workflows(repo_name)

    print(f"===============\nupdating ci runs")
    rate_limits_runs = RepoRatelimits(repo_names)
    for repo_name in repo_names:
        update_runs(repo_name, rate_limits_runs)

    print(f"===============\nupdating ci jobs")
    print("-- updating ci jobs temporarily disabled --")
    # rate_limits_jobs = RepoRatelimits(repo_names)
    # for repo_name in repo_names:
    #     update_jobs(repo_name, rate_limits_jobs)


def update_workflows(github_repo):
    print(f"updating workflows for: {github_repo}")
    assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", github_repo), "regex not matched"  # format: 'org/repo_name'
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
    assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", github_repo), "regex not matched"  # format: 'org/repo_name'
    rate_limit = rate_limits.get_repo_rate_limit(github_repo)
    if rate_limit == 0:
        print("skipping - rate limit 0")
        return
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
    assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", github_repo), "regex not matched"  # format: 'org/repo_name'
    rate_limit = rate_limits.get_repo_rate_limit(github_repo)

    # first get the run_ids, we need them to fetch the jobs
    with DuckLakeConnection() as con:
        assert con.table_exists(GITHUB_RUNS_TABLE), f"tabel {GITHUB_RUNS_TABLE} does not exist"
        create_table, is_first_run = get_jobs_table_state(con, github_repo)
        run_ids_count = (
            get_run_ids_count(con, github_repo)
            if is_first_run
            else get_recent_run_ids_without_jobs_count(con, github_repo)
        )
        print(f"jobs need to be fetched for {run_ids_count} runs for repo {github_repo}")
        if run_ids_count > 0:
            if run_ids_count > rate_limit:
                print(f"applying rate limit: fetching jobs for {rate_limit} runs")
            run_ids = (
                get_run_ids(con, github_repo, rate_limit)
                if is_first_run
                else get_recent_run_ids_without_jobs(con, github_repo, rate_limit)
            )
        else:
            run_ids = []
    # fetch jobs from github
    new_jobs = []
    if run_ids:
        print('fetching jobs per run:')
    total_runs = len(run_ids)
    count = 1
    for (run_id,) in run_ids:
        print(f"{count}/{total_runs}", flush=True)
        endpoint = GITHUB_JOBS_ENDPOINT.format(GITHUB_REPO=github_repo, RUN_ID=run_id)
        try:
            _, jobs = fetch_github_record_list(endpoint, 'jobs', rate_limit, detail_log=True)
            new_jobs.extend(jobs)
        except (ValueError) as e:
            print(f"::info {e}")
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
                print('created table and stored workflows:')
                con.sql(f"select * from {GITHUB_WORKFLOWS_TABLE} order by id").show()
            else:
                con.execute(
                    f"""
                    merge into {GITHUB_WORKFLOWS_TABLE}
                    using {subquery} as upserts
                    on ({GITHUB_WORKFLOWS_TABLE}.id = upserts.id
                      and {GITHUB_WORKFLOWS_TABLE}.repository = upserts.repository)
                    when matched then update
                    when not matched then insert;
                    """
                )
                # NOTE: we assume a new snapshot is created after every 'merge into', even if there are no changes, see: https://github.com/duckdblabs/duckdb-internal/issues/6557
                current_snapshot = con.current_snapshot()
                rel = con.table_changes(GITHUB_WORKFLOWS_TABLE, current_snapshot, current_snapshot)
                if rel.fetchone():
                    print("new or updated workflows:")
                    rel.show()
                else:
                    print('no updates')


def store_runs(runs, create_table, latest_previously_stored, max_age: int | None = GITHUB_RUNS_STALE_DELAY):
    runs_str = f"[{',\n'.join([json.dumps(r) for r in runs])}]"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json") as tmp:
        tmp.write(runs_str)
        tmp.flush()
        with DuckLakeConnection() as con:
            # subquery to fetch only consecutive completed runs (i.e. no 'queued' or 'in progress' in between)
            # Note: max_age age can be set to to filter out stale runs.
            stale_timestamp = (
                (datetime.now() - timedelta(hours=max_age)).strftime("%Y-%m-%d %H:%M:%S") if max_age else None
            )
            oldest_non_completed = con.sql(
                f"""select min(id) from read_json('{tmp.name}') where status != 'completed'
                {f"and updated_at > TIMESTAMP '{stale_timestamp}'" if stale_timestamp else ''}
                """
            ).fetchone()[0]
            subquery = f"""
                        (
                        select * from read_json('{tmp.name}')
                        where True
                        {f"and id < {oldest_non_completed}" if oldest_non_completed else ''}
                        {f"and id > {latest_previously_stored}" if latest_previously_stored else ''}
                        )
                        """
            if not con.sql(f"from {subquery} limit 1").fetchone():
                print("no new runs to store")
                return
            if create_table:
                con.execute(f"create table {GITHUB_RUNS_TABLE} as {subquery}")
            else:
                con.execute(f"insert into {GITHUB_RUNS_TABLE} {subquery}")
            print('stored runs:')
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
            print('stored jobs:')
            con.sql(f"select * from {subquery} order by id").show()


if __name__ == "__main__":
    run()
