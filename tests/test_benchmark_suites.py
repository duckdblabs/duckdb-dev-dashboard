"""New suites appear on both overview and query-history pages without page-specific lists."""

import re
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SUITES_SQL = (ROOT / "evidence/sources/benchmarks/benchmark_series_list.sql").read_text()


class BenchmarkSuitesTest(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        self.con.execute("""
            create table benchmark_geomean (
                storage_type varchar, benchmark_series varchar, benchmark varchar,
                scale_factor double
            );
            insert into benchmark_geomean values
                ('duckdb', 'tpcds @ sf100', 'tpcds', 100),
                ('duckdb', 'tpch @ sf100', 'tpch', 100),
                ('duckdb', 'clickbench', 'clickbench', null),
                ('ducklake', 'tpcds @ sf100', 'tpcds', 100);
            create schema benchmarks;
        """)

    def refresh(self):
        self.con.execute("create or replace table benchmarks.benchmark_series_list as " + SUITES_SQL)

    def options(self, storage, history=False):
        path = (ROOT / f"evidence/pages/benchmarks/{storage}/queries.md" if history
                else ROOT / f"evidence/pages/benchmarks/{storage}.md")
        sql = re.search(r"```sql suite_options\n(.*?)\n```", path.read_text(), re.S).group(1)
        return self.con.execute(sql).fetchall()

    def test_suite_arrives_only_on_the_storage_backend_that_has_results(self):
        self.refresh()
        self.assertNotIn(("h2oai", "H2O.ai (1 billion rows)"), self.options("duckdb"))
        self.con.execute("""
            insert into benchmark_geomean values
                ('duckdb', 'h2oai', 'h2oai', null),
                ('duckdb', 'h2oai', 'h2oai', null);
        """)
        self.refresh()
        for history in (False, True):
            with self.subTest(history=history):
                self.assertEqual(self.options("duckdb", history).count(
                    ("h2oai", "H2O.ai (1 billion rows)")), 1)
                self.assertEqual(self.options("ducklake", history),
                                 [("tpcds @ sf100", "TPC-DS @ sf100")])

    def test_h2oai_and_future_suites_appear_on_overviews_and_in_dropdowns(self):
        for storage in ("duckdb", "ducklake"):
            self.con.execute("""
                insert into benchmark_geomean values
                    (?, 'h2oai', 'h2oai', null),
                    (?, 'future-suite @ sf10', 'future-suite', 10);
            """, [storage, storage])
        self.refresh()
        for storage in ("duckdb", "ducklake"):
            for history in (False, True):
                with self.subTest(storage=storage, history=history):
                    options = self.options(storage, history)
                    self.assertIn(("h2oai", "H2O.ai (1 billion rows)"), options)
                    self.assertIn(("future-suite @ sf10", "future-suite @ sf10"), options)

    def test_overview_preserves_existing_order_and_formats_other_scale_factors(self):
        self.con.execute("""
            insert into benchmark_geomean values
                ('duckdb', 'h2oai', 'h2oai', null),
                ('duckdb', 'tpch @ sf10', 'tpch', 10);
        """)
        self.refresh()
        self.assertEqual(self.options("duckdb"), [
            ("tpcds @ sf100", "TPC-DS @ sf100"),
            ("tpch @ sf10", "TPC-H @ sf10"),
            ("tpch @ sf100", "TPC-H @ sf100"),
            ("clickbench", "ClickBench"),
            ("h2oai", "H2O.ai (1 billion rows)")
        ])


if __name__ == "__main__":
    unittest.main()
