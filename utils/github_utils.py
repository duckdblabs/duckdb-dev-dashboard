import os
import requests


def gh_api_request(url, headers = {"Accept": "application/vnd.github+json"}, params=None) -> dict:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    resp = requests.get(url=url, headers=headers, params=params)
    if resp.status_code != 200:
        raise ValueError(f"fetching from: {url}\n GitHub API error: {resp.status_code} {resp.text}")
    return resp.json()


def get_rate_limit():
    url = "https://api.github.com/rate_limit"
    resp = gh_api_request(url)
    rate_limit = resp['rate']['remaining']
    if not isinstance(rate_limit, int):
        raise ValueError(f"Invalid rate limit: {rate_limit}")
    return rate_limit


def fetch_repo_names(organisation: str) -> list[str]:
    url = f"https://api.github.com/orgs/{organisation}/repos"
    per_page = 100  # max value for github api
    page = 1
    repos = []
    while True:
        params = {"per_page": per_page, "page": page}
        resp = gh_api_request(url, params=params)
        repos.extend(repo['full_name'] for repo in resp)
        if len(resp) < per_page:
            break
        page += 1
    return repos


def fetch_github_record_list(
    endpoint, main_node, rate_limit=None, reference_id=0, fetch_smaller=False, start_page=1, detail_log=False
):
    """
    Fetch records from a github list API
    Requirements the endpoint is assumed to meet:
        - support for query parameters: 'page', 'per_page'
        - the response object has key 'total_count' and key (main_node) that maps to an array of objects
        - the objects have key 'id'

    Args:
        endpoint: the github api endpoint, e.g. 'https://api.github.com/repos/duckdb/duckdb/actions/runs'
        main_node: github api (typically) returns 2 nodes: 'total_count' and a main node that contains the actual records
        reference_id: a reference id that is used to limit the number of records to fetch;
            - value 0 means: fetch all records from endpoint
            - NOTE: only use this argument when the API resuls are ordered DESC by 'id' !!
        fetch_smaller:
            - if True, only include records with ids <= reference_id (i.e. less recent)
            - if False (default), only include records with ids >= reference_id (i.e. more recent)
        rate_limit: max number of API calls (i.e. max number of pages)
        start_page: pages=1 means start with most recent data; other number means we skip data (100 records per page)

    Returns: tuple with 2 values:
        total_count: total nr of records available at the endpoint
        List[dict]: a list of queried records with ids >= reference_id (i.e. more recent); except if fetch_smaller is set
    """

    fetched_records = []
    page = start_page
    per_page = 100  # max value for github api
    total_count = 0
    if detail_log:
        print(f"fetching from: {endpoint}")
    while True:
        if detail_log and page > 1:
            print(f"page: {page}", flush=True)
        resp = gh_api_request(endpoint, params={"per_page": per_page, "page": page})
        if 'total_count' not in resp or main_node not in resp:
            raise ValueError(f"unexpected response keys: {resp.keys()};\nexpected: 'total_count' and '{main_node}'")
        if page == start_page:
            total_count = resp['total_count']
        data = resp[main_node]
        if reference_id == 0:
            fetched_records.extend(data)
        elif reference_id != 0 and data != []:
            # we take advantage of the fact that gh returns the records in DESC order (most recent first, which have highest ids)
            if data[-1]['id'] >= reference_id:
                # all more recent then reference_id
                if not fetch_smaller:
                    fetched_records.extend(data)
            elif data[0]['id'] <= reference_id:
                # all less recent then reference_id
                if fetch_smaller:
                    fetched_records.extend(data)
                else:
                    break
            else:
                # reference_id intersects fetched id range
                try:
                    reference_idx = [rec['id'] for rec in data].index(reference_id)
                except ValueError:
                    raise ValueError(f"Error: reference_id: {reference_id} not found")
                if fetch_smaller:
                    fetched_records.extend(data[reference_idx:])
                else:
                    fetched_records.extend(data[: reference_idx + 1])
                    break
        if len(data) < per_page:
            break
        if rate_limit and page - start_page > rate_limit:
            break
        page += 1
    if detail_log:
        print(f"fetched {len(fetched_records)} from a total of {total_count} records")
    return total_count, fetched_records
