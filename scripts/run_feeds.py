import subprocess
from pathlib import Path


def run_all_feeds():
    feeds_dir = Path(__file__).parent / 'feeds'
    for file in feeds_dir.iterdir():
        if file.is_file() and file.suffix == '.py':
            print(f"Running {file.name}...", flush=True)
            result = subprocess.run(['python3', str(file)], capture_output=True, text=True, cwd=str(feeds_dir))
            print(result.stdout, flush=True)
            if result.stderr or result.returncode != 0:
                error_str = f"{file.name}:\n{result.stderr}\nreturn code: {result.returncode}"
                print(f"Error when running {error_str}", flush=True)


if __name__ == "__main__":
    run_all_feeds()
