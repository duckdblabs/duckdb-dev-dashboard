"""Keep isolated query failures, but exclude wholly failed benchmark runs."""

import unittest
from pathlib import Path

import duckdb


QUERY = (Path(__file__).resolve().parents[1]
         / "benchmark_derived_tables/benchmark_query_times.sql").read_text()


class BenchmarkQueryTimesTest(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        self.con.execute("""
            create table runs (
                run_id varchar, timestamp timestamp, merge_commit_date timestamp,
                benchmark varchar, benchmark_name varchar, storage_type varchar,
                scale_factor double, cpu_arch varchar, machine_type varchar, os varchar,
                duckdb_version varchar, duckdb_commit_sha varchar, queries_sha varchar,
                is_test boolean
            );
            create table query_results (
                run_id varchar, query varchar, status varchar, verified boolean,
                error varchar, median_seconds double
            );
            create table query_metrics (
                run_id varchar, query varchar, metric_name varchar, metric_value double
            );
        """)
        # Both attempts use the same version and commit. Only the rerun measured anything.
        self.add_run("whole-failure", [("q01", "failed"), ("q23", "failed"), ("q67", "failed")])
        self.add_run("partial-rerun", [("q01", "ok"), ("q23", "failed"), ("q67", "failed")])
        self.add_run("success", [("q01", "ok"), ("q23", "ok"), ("q67", "ok")])
        self.add_run("test", [("q01", "ok")], is_test=True)

    def add_run(self, run_id, queries, *, is_test=False):
        self.con.execute("""
            insert into runs values (
                ?, '2026-09-22', '2026-09-18', 'tpcds', 'TPC-DS', 'ducklake',
                100, 'arm64', 'c7gd.4xlarge', 'linux', 'v2.0.0-alpha42526',
                'same-commit', 'same-query-set', ?
            )
        """, [run_id, is_test])
        for query, status in queries:
            ok = status == "ok"
            self.con.execute("insert into query_results values (?, ?, ?, ?, ?, ?)",
                             [run_id, query, status, ok,
                              None if ok else "DuckDB exited with status -9",
                              1 if ok else None])
            if ok:
                self.con.execute("""
                    insert into query_metrics values (?, ?, 'execution_time_seconds', 1)
                """, [run_id, query])

    def rows(self):
        cursor = self.con.execute(QUERY)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def test_excludes_wholly_failed_runs_but_keeps_their_partial_rerun(self):
        rows = self.rows()
        self.assertEqual({row["run_id"] for row in rows}, {"partial-rerun", "success"})
        self.assertEqual(len(rows), 6)

    def test_partial_run_preserves_query_errors_without_fabricating_timings(self):
        rows = [row for row in self.rows() if row["run_id"] == "partial-rerun"]
        by_query = {row["query"]: row for row in rows}
        self.assertEqual(by_query["q01"]["mean_seconds"], 1)
        for query in ("q23", "q67"):
            self.assertEqual(by_query[query]["status"], "failed")
            self.assertEqual(by_query[query]["error"], "DuckDB exited with status -9")
            self.assertIsNone(by_query[query]["mean_seconds"])
            self.assertIsNone(by_query[query]["median_seconds"])
            self.assertIsNone(by_query[query]["timed_runs"])

    def test_stray_metric_does_not_make_a_wholly_failed_run_eligible(self):
        self.con.execute("""
            insert into query_metrics values
                ('whole-failure', 'q01', 'execution_time_seconds', 1);
        """)
        self.assertNotIn("whole-failure", {row["run_id"] for row in self.rows()})


if __name__ == "__main__":
    unittest.main()
