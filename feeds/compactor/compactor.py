
from utils.ducklake import DuckLakeConnection

def run(dl_secret: str):
    with DuckLakeConnection(dl_secret) as con:
        con.execute("SET memory_limit = '8GB'")
        print('ducklake_merge_adjacent_files 1', flush=True)
        con.execute(f"CALL ducklake_merge_adjacent_files('my_ducklake', 'ci_jobs')")
        print('ducklake_merge_adjacent_files 2', flush=True)
        con.execute(f"CALL ducklake_merge_adjacent_files('my_ducklake', 'ci_repositories')")
        print('ducklake_merge_adjacent_files 3', flush=True)
        con.execute(f"CALL ducklake_merge_adjacent_files('my_ducklake', 'ci_repositories_metadata')")
        print('ducklake_merge_adjacent_files 4', flush=True)
        con.execute(f"CALL ducklake_merge_adjacent_files('my_ducklake', 'ci_runs')")
        print('ducklake_merge_adjacent_files 5', flush=True)
        con.execute(f"CALL ducklake_merge_adjacent_files('my_ducklake', 'ci_workflows')")
        print('ducklake_merge_adjacent_files 6', flush=True)
        con.execute(f"CALL ducklake_merge_adjacent_files('my_ducklake', 'extension_downloads')")


if __name__ == "__main__":
    run()
