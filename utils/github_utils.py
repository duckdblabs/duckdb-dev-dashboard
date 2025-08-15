import os
import requests
import sys

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


def get_rate_limit():
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    resp = requests.get('https://api.github.com/rate_limit', headers=headers)
    if resp.status_code != 200:
        print(f"GitHub API error: {resp.status_code} {resp.text}")
        exit(1)
    rate_limit = resp.json()['rate']['remaining']
    if not isinstance(rate_limit, int):
        print(f"Invalid rate limit: {resp.status_code} {resp.text}")
        exit(1)
    return rate_limit


def fetch_github_record_list(
    url, main_node, rate_limit, reference_id=0, fetch_smaller=False, start_page=1, detail_log=False
):
    """
    Fetch records from a github list API
    Requirements the endpoint is assumed to meet:
        - support for query parameters: 'page', 'per_page'
        - the response object has key 'total_count' and key (main_node) that maps to an array of objects
        - the objects have key 'id'

    Args:
        url: the github api endpoint, e.g. 'https://api.github.com/repos/duckdb/duckdb/actions/runs'
        main_node: github api (typically) returns 2 nodes: 'total_count' and a main node that contains the actual records
        reference_id: a reference id that is used to limit the number of records to fetch;
            - value 0 means: fetch all records from endpoint
            - NOTE: only use this argument when the API resuls are ordered DESC by 'id' !!
        fetch_smaller:
            - if True, only include records with ids <= reference_id (i.e. less recent)
            - if False (default), only include records with ids >= reference_id (i.e. more recent)
        rate_limit: max number of API calls (i.e. max number of pages)
        start_page: pages=1 means start with most recent data; other number means we skip data (100 records per page)

    Returns: tuple with 3 values:
        total_count: total nr of records available at the endpoint
        List[dict]: a list of queried records with ids >= reference_id (i.e. more recent); except if fetch_smaller is set
        error: error msg (in case of invalid api response)
    """

    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    fetched_records = []

    page = start_page
    per_page = 100  # max value for github api
    total_count = 0
    if detail_log:
        print(f"fetching from: {url}")
    while True:
        if detail_log and page > 1:
            print(f"page: {page}")
        params = {"per_page": per_page, "page": page}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            error = f"fetching from: {url}\nGitHub API error: {resp.status_code} {resp.text}"
            return 0, [], error
        resp_data: dict = resp.json()
        if 'total_count' not in resp_data or main_node not in resp_data:
            print(f"unexpected response keys: {resp_data.keys()};\nexpected: 'total_count' and '{main_node}'")
            exit(1)
        if page == start_page:
            total_count = resp_data['total_count']
        data = resp_data[main_node]
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
                    print(f"Error: reference_id: {reference_id} not found")
                    exit(1)
                if fetch_smaller:
                    fetched_records.extend(data[reference_idx:])
                else:
                    fetched_records.extend(data[: reference_idx + 1])
                    break
        if len(data) < per_page:
            break
        if page - start_page > rate_limit:
            break
        page += 1
    if detail_log:
        print(f"fetched {len(fetched_records)} from a total of {total_count} records")
    return total_count, fetched_records, ""
