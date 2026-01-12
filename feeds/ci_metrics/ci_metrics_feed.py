import json
import tempfile
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

from utils.ducklake import DuckLakeConnection
from utils.github_utils import fetch_github_record_list, fetch_github_records
from .ci_metrics_utils import RepoRatelimits, fetch_github_actions_runs, get_recent_run_ids_without_jobs
from .ci_config import *

load_dotenv()


def run():
    with DuckLakeConnection() as con:
        print(f"===============\nupdating repositories")
        repo_names = update_repositories(con)
        print(f"===============\nupdating ci workflows")
        update_workflows(repo_names, con)

    print(f"===============\nupdating ci runs")
    update_runs(repo_names)

    print(f"===============\nupdating ci jobs")
    update_jobs(repo_names)


def update_repositories(con: DuckLakeConnection) -> list[str]:
    repos = fetch_github_records(GITHUB_REPOS_ENDPOINT)
    if not repos:
        raise ValueError(f"No repositories could be fetched at endpoint: {GITHUB_REPOS_ENDPOINT}'")
    if con.table_exists(GITHUB_REPOS_TABLE) and con.table_empty(GITHUB_REPOS_TABLE):
        raise ValueError(f"Invalid state - Table {GITHUB_REPOS_TABLE} should not be empty")
    con.create_table(GITHUB_REPOS_TABLE, repos, if_not_exists=True, with_no_data=True)
    con.upsert_table(GITHUB_REPOS_TABLE, repos, ['id'], print_changes=True)
    repo_names = [repo['full_name'] for repo in repos]
    return repo_names


def update_workflows(github_repos: list[str], con: DuckLakeConnection):
    all_workflows = []
    for github_repo in github_repos:
        assert re.fullmatch(
            r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", github_repo
        ), f"invalid org/repo_name: '{github_repo}'"  # format: 'org/repo_name'
        endpoint = GITHUB_WORKFLOWS_ENDPOINT.format(GITHUB_REPO=github_repo)
        _, workflows = fetch_github_record_list(endpoint, 'workflows', detail_log=True)
        for workflow in workflows:
            workflow['repository'] = github_repo
        all_workflows.extend(workflows)
    if all_workflows:
        if con.table_exists(GITHUB_WORKFLOWS_TABLE):
            if con.table_empty(GITHUB_WORKFLOWS_TABLE):
                raise ValueError(f"Invalid state - Table {GITHUB_WORKFLOWS_TABLE} should not be empty")
            print(f"upserting into {GITHUB_WORKFLOWS_TABLE}")
            con.upsert_table(GITHUB_WORKFLOWS_TABLE, all_workflows, ['id', 'repository'], True)
        else:
            con.create_table(GITHUB_WORKFLOWS_TABLE, all_workflows)
    else:
        print(f"no workflows found")


def update_runs(github_repos: list[str]):
    # get ducklake state
    with DuckLakeConnection() as con:
        if con.table_exists(GITHUB_RUNS_TABLE):
            if con.table_empty(GITHUB_RUNS_TABLE):
                raise ValueError(f"Invalid state - Table {GITHUB_RUNS_TABLE} should not be empty")
            create_table = False
        else:
            create_table = True
        # fetch previous max run id per repo from ducklake
        query = f"""
        SELECT
          repos.full_name,
          max(runs.id)
        FROM {GITHUB_REPOS_TABLE} repos
          LEFT JOIN {GITHUB_RUNS_TABLE} runs on runs.repository['id'] = repos.id
        GROUP BY repos.full_name
        ORDER BY repos.full_name
        """
        res = con.sql(query).fetchall()
        repo_max_run_id = {tup[0]: tup[1] for tup in res}

    # fetch from gh api store in ducklake
    rate_limits = RepoRatelimits(github_repos)
    for github_repo in github_repos:
        rate_limit = rate_limits.get_repo_rate_limit(github_repo)
        assert github_repo in repo_max_run_id
        print(f"current max(id) for {github_repo} in {GITHUB_RUNS_TABLE}: {repo_max_run_id[github_repo]}")
        runs = fetch_github_actions_runs(rate_limit, github_repo, repo_max_run_id[github_repo])
        if runs:
            store_runs(runs, create_table, repo_max_run_id[github_repo])
        create_table = False


def update_jobs(github_repos: list[str]):
    rate_limits = RepoRatelimits(github_repos)
    # get runs without jobs
    with DuckLakeConnection() as con:
        assert con.table_exists(GITHUB_RUNS_TABLE), f"tabel {GITHUB_RUNS_TABLE} does not exist"
        if con.table_exists(GITHUB_JOBS_TABLE):
            if con.table_empty(GITHUB_JOBS_TABLE):
                raise ValueError(f"Invalid state - Table {GITHUB_JOBS_TABLE} should not be empty")
            create_table = False
        else:
            create_table = True
        repo_runs: dict[str, list] = get_recent_run_ids_without_jobs(con)

    # fetch jobs for runs without jobs
    for github_repo in github_repos:
        assert (
            github_repo in repo_runs
        ), f"repo {github_repo} not found in query output: 'get_recent_run_ids_without_jobs'"
        rate_limit = rate_limits.get_repo_rate_limit(github_repo)
        run_ids = repo_runs[github_repo]
        run_ids_count = len(run_ids)
        print(f"jobs need to be fetched for {run_ids_count} runs for repo {github_repo}")
        if run_ids_count > rate_limit:
            print(f"applying rate limit: fetching jobs for {rate_limit} runs")

        # fetch jobs from github
        new_jobs = []
        if run_ids:
            print('fetching jobs per run:')
        total_runs = min(run_ids_count, rate_limit)
        for idx, run_id in enumerate(run_ids):
            if idx + 1 > rate_limit:
                break
            print(f"{idx + 1}/{total_runs}", flush=True)
            endpoint = GITHUB_JOBS_ENDPOINT.format(GITHUB_REPO=github_repo, RUN_ID=run_id)
            try:
                _, jobs = fetch_github_record_list(endpoint, 'jobs', rate_limit, detail_log=True)
                new_jobs.extend(jobs)
            except ValueError as e:
                print(f"::notice title=could not fetch job::endpoint: '{endpoint}'; Error: {e}")

        # store in ducklake
        if new_jobs:
            store_jobs(new_jobs, create_table)
            create_table = False


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
