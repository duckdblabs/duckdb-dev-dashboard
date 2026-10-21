"""Exercise the pages' baseline SQL without credentials or a live benchmark lake.

Run with: python3 -m unittest discover -s tests
"""

import re
import unittest
from pathlib import Path

import duckdb


PAGES = Path(__file__).resolve().parents[1] / "evidence/pages/benchmarks"


def page_query(storage, name, history=False):
    path = PAGES / storage / "queries.md" if history else PAGES / f"{storage}.md"
    sql = re.search(rf"```sql {name}\n(.*?)\n```", path.read_text(), re.S).group(1)
    for dependency in ("platform_options", "geomean", "query_history", "run_options"):
        sql = sql.replace("${" + dependency + "}", dependency)
    return (sql.replace("${inputs.platform_select.value}", "linux|x86_64|test")
            .replace("${inputs.suite_select.value}", "tpch @ sf100")
            .replace("${inputs.run_select.value}", f"{storage}-selected"))


class BenchmarkBaselinesTest(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        self.con.execute("""
            create schema benchmarks;
            create table benchmarks.geomean_runs (
                run_id varchar, storage_type varchar, benchmark_series varchar,
                duckdb_version varchar, run_timestamp timestamp, merge_commit_date timestamp,
                geomean_seconds double, machine_label varchar, cpu_arch_label varchar,
                os varchar, queries_sha varchar
            );
            create table benchmarks.query_times (
                run_id varchar, storage_type varchar, benchmark_series varchar,
                duckdb_version varchar, run_timestamp timestamp, query varchar,
                median_seconds double, mean_seconds double, timed_runs int, status varchar,
                machine_label varchar, cpu_arch_label varchar, os varchar, queries_sha varchar
            );
            create table platform_options as
                select 'linux|x86_64|test' as platform_id, 'test' as machine_label,
                       'x86_64' as cpu_arch_label, 'linux' as os;
            create table run_options (run_id varchar, run_timestamp timestamp);
            create table query_history (query varchar, queries_sha varchar);
            insert into query_history values
                ('slow', 'queries-a'), ('fast', 'queries-a'), ('neutral', 'queries-a'),
                ('zero', 'queries-a'), ('failed', 'queries-a');
        """)
        for storage in ("duckdb", "ducklake"):
            for version in ("v1.4.5", "v1.5.5", "v2.0.0", "v2.0.0-alpha1", "v2.1.0-alpha2"):
                # DuckLake has no 2.0.0 release measurement yet.
                if storage == "ducklake" and version == "v2.0.0":
                    continue
                merge_date = "2026-10-20" if "alpha" in version else None
                self.con.execute("""
                    insert into benchmarks.geomean_runs values
                        (?, ?, 'tpch @ sf100', ?, '2026-09-01', ?, 1,
                         'test', 'x86_64', 'linux', 'queries-a')
                """, [f"{storage}-{version}", storage, version, merge_date])
                for query in ("slow", "fast", "neutral", "zero", "failed"):
                    seconds = 0 if query == "zero" else (2 if version == "v1.5.5" else 1)
                    self.query_row(storage, f"{storage}-{version}", version, query, seconds)
            self.con.execute("insert into run_options values (?, '2026-10-20')",
                             [f"{storage}-selected"])
            for query, seconds in (("slow", 1.2), ("fast", 0.8), ("neutral", 1.1),
                                   ("zero", 1), ("failed", None)):
                self.query_row(storage, f"{storage}-selected", "v2.1.0-alpha2", query, seconds)
        self.con.execute("""
            create table geomean as
                select * from benchmarks.geomean_runs where merge_commit_date is not null;
        """)

    def query_row(self, storage, run_id, version, query, seconds, *,
                  machine="test", query_set="queries-a", timestamp="2026-09-01"):
        self.con.execute("""
            insert into benchmarks.query_times values
                (?, ?, 'tpch @ sf100', ?, ?, ?, ?, ?, 10, ?, ?, 'x86_64', 'linux', ?)
        """, [run_id, storage, version, timestamp, query, seconds, seconds,
              "failed" if seconds is None else "ok", machine, query_set])

    def result(self, storage, name, history=False):
        cursor = self.con.execute(page_query(storage, name, history))
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def test_release_lines_still_include_both_development_branches(self):
        for storage in ("duckdb", "ducklake"):
            for history in (False, True):
                with self.subTest(storage=storage, history=history):
                    self.assertEqual(
                        [row["version_line"] for row in self.result(storage, "version_options", history)],
                        ["v2.1", "v2.0"])

    def test_suite_baselines_include_release_outside_the_development_window(self):
        # A newer measurement of the same release must replace the older one.
        self.con.execute("""
            insert into benchmarks.geomean_runs
                select 'new-release', storage_type, benchmark_series, duckdb_version,
                       '2026-09-02', null, 3, machine_label, cpu_arch_label, os, queries_sha
                from benchmarks.geomean_runs
                where storage_type = 'duckdb' and duckdb_version = 'v2.0.0';
        """)
        for storage in ("duckdb", "ducklake"):
            for history in (False, True):
                with self.subTest(storage=storage, history=history):
                    baselines = {row["duckdb_version"]: row["baseline_seconds"]
                                 for row in self.result(storage, "version_baselines", history)}
                    self.assertEqual(baselines, {"v1.4.5": 1, "v1.5.5": 1,
                                                **({"v2.0.0": 3} if storage == "duckdb" else {})})

    def test_query_baselines_match_platform_and_query_set(self):
        self.query_row("duckdb", "new-release", "v2.0.0", "slow", 3, timestamp="2026-09-02")
        self.query_row("duckdb", "wrong-platform", "v2.0.0", "slow", 99,
                       machine="other", timestamp="2026-10-21")
        self.query_row("duckdb", "wrong-query-set", "v2.0.0", "slow", 99,
                       query_set="queries-b", timestamp="2026-10-21")
        baselines = {(row["query"], row["duckdb_version"]): row["baseline_seconds"]
                     for row in self.result("duckdb", "query_baselines", history=True)}
        self.assertEqual(baselines["slow", "v2.0.0"], 3)
        self.assertFalse(any("alpha" in version for _, version in baselines))
        self.assertFalse(any(row["duckdb_version"] == "v2.0.0"
                             for row in self.result("ducklake", "query_baselines", history=True)))

    def test_run_table_ratios_colours_and_missing_release(self):
        for storage in ("duckdb", "ducklake"):
            with self.subTest(storage=storage):
                rows = self.result(storage, "query_times")
                by_query = {row["query"]: row for row in rows}
                self.assertEqual(len(rows), 5)  # Failed and zero-baseline queries remain visible.
                self.assertEqual(rows[0]["query"], "slow")
                self.assertEqual(by_query["slow"]["ratio vs v1.5.5"], 0.6)
                self.assertIsNone(by_query["zero"]["ratio vs v2.0.0"])
                self.assertIsNone(by_query["failed"]["ratio vs v2.0.0"])
                if storage == "duckdb":
                    self.assertEqual(by_query["slow"]["v2.0.0 (s)"], 1)
                    self.assertEqual(by_query["slow"]["ratio vs v2.0.0"], 1.2)
                    self.assertEqual(by_query["slow"]["v2.0.0 color"], 1)
                    self.assertEqual(by_query["fast"]["v2.0.0 color"], -1)
                    self.assertIsNone(by_query["neutral"]["v2.0.0 color"])
                else:
                    self.assertTrue(all(row["v2.0.0 (s)"] is None for row in rows))
                    self.assertTrue(all(row["v2.0.0 color"] is None for row in rows))

    def test_ducklake_release_appears_when_measurements_arrive(self):
        self.con.execute("""
            insert into benchmarks.geomean_runs
                select 'ducklake-release', 'ducklake', benchmark_series, duckdb_version,
                       run_timestamp, merge_commit_date, geomean_seconds,
                       machine_label, cpu_arch_label, os, queries_sha
                from benchmarks.geomean_runs
                where storage_type = 'duckdb' and duckdb_version = 'v2.0.0';
        """)
        self.query_row("ducklake", "ducklake-release", "v2.0.0", "slow", 1)
        for history in (False, True):
            baselines = self.result("ducklake", "version_baselines", history)
            self.assertTrue(any(row["duckdb_version"] == "v2.0.0" for row in baselines))
        baselines = self.result("ducklake", "query_baselines", history=True)
        self.assertTrue(any(row["duckdb_version"] == "v2.0.0" for row in baselines))
        slow = next(row for row in self.result("ducklake", "query_times") if row["query"] == "slow")
        self.assertEqual(slow["ratio vs v2.0.0"], 1.2)


if __name__ == "__main__":
    unittest.main()
