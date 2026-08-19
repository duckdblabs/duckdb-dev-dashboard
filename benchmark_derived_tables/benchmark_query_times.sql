-- Per-query execution time, one row per (run, query).
--
-- This is the level below benchmark_geomean.sql: that file aggregates the warm runs of a query
-- into one number and then collapses the queries into a single geomean per run; this one stops
-- after the first step, so an individual query can be inspected and compared across runs.
--
-- mean_seconds is the value that feeds the geomean (benchmark_geomean.sql's per_query CTE).
-- median_seconds is carried alongside it because they answer different questions: a large gap
-- between the two means the warm runs of that query were noisy, which is worth seeing before
-- reading anything into a small change. median_seconds should also agree with the harness's own
-- query_results.median_seconds, which is computed independently over the same timed runs.
--
-- Failed queries are kept, with NULL timings and their error - this is the one place where the
-- failure is attributable to a specific query, so hiding them here would waste the table.
-- benchmark_geomean.sql filters them out instead, because a failed query has no timing to
-- aggregate.
--
-- Size: one row per query per run, i.e. roughly 1/30th of query_metrics (which holds one row per
-- query per warm run per metric). Small enough to ship to the browser; query_metrics is not.

with warm_runs as (
    -- aggregate the timed runs (query_run 1..warm_runs) of each query
    select
        qm.run_id,
        qm.query,
        avg(qm.metric_value)    as mean_seconds,
        median(qm.metric_value) as median_seconds,
        min(qm.metric_value)    as fastest_seconds,
        max(qm.metric_value)    as slowest_seconds,
        count(*)                as timed_runs
    from query_metrics qm
    where qm.metric_name = 'execution_time_seconds'
      and qm.metric_value is not null
      and qm.metric_value > 0    -- a non-positive timing is not a measurement
    group by qm.run_id, qm.query
)
select
    r.run_id,
    r."timestamp"       as run_timestamp,
    strftime(r."timestamp", '%Y-%m-%d %H:%M') as run_date,
    r.benchmark,
    r.benchmark_name,
    r.storage_type,     -- one dashboard page per value
    r.scale_factor,
    coalesce('sf' || printf('%g', r.scale_factor), 'n/a')                as scale_factor_label,
    r.benchmark || coalesce(' @ sf' || printf('%g', r.scale_factor), '') as benchmark_series,
    coalesce(r.cpu_arch, 'unknown')      as cpu_arch_label,
    coalesce(r.machine_type, 'unspecified') as machine_label,
    r.duckdb_version,
    r.duckdb_commit_sha,
    r.queries_sha,
    qr.query,
    qr.status,          -- 'ok' or 'failed'
    qr.verified,        -- false when the query was timed but its result never checked
    qr.error,
    w.median_seconds,
    w.mean_seconds,
    w.fastest_seconds,
    w.slowest_seconds,
    w.timed_runs
from runs r
join query_results qr
    on qr.run_id = r.run_id
-- left join: a failed query has no rows in query_metrics but must still appear
left join warm_runs w
    on w.run_id = qr.run_id
   and w.query = qr.query
where not coalesce(r.is_test, false)
order by r."timestamp", r.benchmark, qr.query;
