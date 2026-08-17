-- Geometric mean of per-query execution time, one row per benchmark run.
--
-- Runs against the benchmark results lake (s3://duckdb-benchmark-lake).
--
-- Two levels of aggregation, deliberately different:
--   1. MEAN over the warm runs of one query -> one representative time per (run_id, query).
--   2. GEOMEAN over the queries of the run  -> one number per run.
--
-- Filters that are not optional:
--   * NOT is_test - trial runs and manual dispatches are recorded rather than withheld, and must
--     be excluded by every consumer.
--   * status='ok' - a failed query has no meaningful timing.
--
-- Comparability caveat: a run in which a query failed has its geomean over a smaller query set
-- than a complete run, and dropping a slow query makes a run look faster. Such runs are still
-- plotted; queries_attempted / queries_failed / is_complete are carried through so the page can
-- show which points are partial. queries_sha is carried for the same reason at the other end:
-- editing a query starts a new, non-comparable series.

with per_query as (
    -- level 1: mean across query_run 1..warm_runs
    select
        qm.run_id,
        qm.query,
        avg(qm.metric_value) as seconds
    from query_metrics qm
    join query_results qr
        on qr.run_id = qm.run_id
       and qr.query = qm.query
    join runs r
        on r.run_id = qm.run_id
    where not coalesce(r.is_test, false)
      and qr.status = 'ok'
      and qm.metric_name = 'execution_time_seconds'
      and qm.metric_value is not null
      and qm.metric_value > 0 
    group by qm.run_id, qm.query
),
per_run as (
    -- level 2: geometric mean across the queries of the run.
    -- greatest(..., 1e-6) is the zero guard: a sub-microsecond timing is the measurement floor,
    -- and a true zero would collapse the whole run's geomean to 0.
    select
        run_id,
        geomean(greatest(seconds, 1e-6)) as geomean_seconds,
        count(*)                         as queries_ok,
        min(seconds)                     as fastest_query_seconds,
        max(seconds)                     as slowest_query_seconds
    from per_query
    group by run_id
),
attempted as (
    -- what the run set out to measure, so a partial run is visible as such
    select
        run_id,
        count(*)                               as queries_attempted,
        count(*) filter (where status <> 'ok') as queries_failed
    from query_results
    group by run_id
)
select
    r.run_id,
    r.invocation_id,
    r."timestamp"       as run_timestamp,  -- aliased: 'timestamp' as a column name trips downstream tooling
    -- date + time, so two runs on the same day are distinguishable in the runs table.
    -- the harness records this as naive UTC. A string, not a timestamp, so it survives to the
    -- front-end at minute precision instead of being re-formatted back to a bare date.
    strftime(r."timestamp", '%Y-%m-%d %H:%M') as run_date,
    r.benchmark,        -- the suite: tpch / tpcds / clickbench
    r.benchmark_name,   -- the config / storage backend: duckdb / ducklake / local / local-ducklake
    r.scale_factor,     -- NULL for clickbench - never filter this with IN
    -- one chart per (benchmark, scale_factor): a single string key makes the page's per-chart
    -- filtering NULL-safe. printf('%g') renders 100.0 as '100', not '100.0'.
    r.benchmark || coalesce(' @ sf' || printf('%g', r.scale_factor), '') as benchmark_series,
    r.duckdb_version,
    r.duckdb_commit_sha,
    r.binary_source,
    r.os,
    r.cpu_arch,
    r.machine_type,
    -- machine_type is only set when the harness is run with --machine-type, and is NULL for every
    -- run currently in the lake. The page filters on this label instead: a NULL would be dropped
    -- by an IN filter, which would silently empty the whole dashboard.
    coalesce(r.machine_type, 'unspecified') as machine_label,
    r.threads,
    r.memory_limit,
    r.storage_type,
    r.warm_runs,
    r.queries_sha,
    p.geomean_seconds,
    p.fastest_query_seconds,
    p.slowest_query_seconds,
    p.queries_ok,
    a.queries_attempted,
    a.queries_failed,
    (a.queries_failed = 0) as is_complete
from runs r
join per_run   p on p.run_id = r.run_id
join attempted a on a.run_id = r.run_id
where not coalesce(r.is_test, false)
order by r."timestamp", r.benchmark, r.benchmark_name;
