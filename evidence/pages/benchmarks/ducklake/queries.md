---
title: DuckLake Benchmark Query History
sidebar_title: Query History
full_width: true
hide_toc: true
---

<script>
	import { page as pageStore } from '$app/stores';
	import { browser as isBrowser } from '$app/environment';

  const allowedSuites = new Set(['tpcds @ sf100', 'tpch @ sf100', 'clickbench']);
  const validDate = (value) => /^\d{4}-\d{2}-\d{2}$/.test(value ?? '')
    && !Number.isNaN(new Date(`${value}T00:00:00Z`).getTime());
  const validPlatform = (value) => /^[a-z0-9._-]+\|[a-z0-9._-]+\|[a-z0-9._-]+$/i.test(value ?? '');

	$: suiteParam = isBrowser ? $pageStore.url.searchParams.get('suite') : null;
	$: platformParam = isBrowser ? $pageStore.url.searchParams.get('platform') : null;
	$: startParam = isBrowser ? $pageStore.url.searchParams.get('start') : null;
	$: endParam = isBrowser ? $pageStore.url.searchParams.get('end') : null;
  $: initialSuite = allowedSuites.has(suiteParam) ? suiteParam : 'tpcds @ sf100';
  $: initialPlatform = validPlatform(platformParam)
    ? platformParam
    : 'linux|x86_64|c6id.4xlarge';
  $: validWindow = validDate(startParam) && validDate(endParam) && startParam <= endParam;
  $: initialStart = validWindow ? startParam : undefined;
  $: initialEnd = validWindow ? endParam : new Date();
</script>

The query plots show the warm-run mean used to calculate the suite geomean. Median and
fastest–slowest timings are included in each point's tooltip as noise context.

```sql suite_options
select
  benchmark_series,
  case benchmark_series
    when 'tpcds @ sf100' then 'TPC-DS @ sf100'
    when 'tpch @ sf100' then 'TPC-H @ sf100'
    when 'clickbench' then 'ClickBench'
    else benchmark_series
  end as suite_label
from (
  select distinct benchmark_series
  from benchmarks.query_times
  where storage_type = 'ducklake'
)
order by suite_label
```

<Dropdown
    name=suite_select
    data={suite_options}
    value=benchmark_series
    label=suite_label
    defaultValue={initialSuite}
    title="Benchmark suite"
/>

```sql date_options
select merge_commit_date::date as merge_commit_date
from benchmarks.query_times
where storage_type = 'ducklake'
  and merge_commit_date is not null
```

<DateRange
    name=date_select
    data={date_options}
    dates=merge_commit_date
    start={initialStart}
    end={initialEnd}
    defaultValue={validWindow ? undefined : 'Last 90 Days'}
/>

<br>

```sql platform_options
select platform_id, os, cpu_arch_label, machine_label, platform_label, memory_label
from benchmarks.platforms
where storage_type = 'ducklake'
order by platform_label collate nocase, platform_label
```

```sql selected_platform
select memory_label
from ${platform_options}
where platform_id = '${inputs.platform_select.value}'
```

<Dropdown
    name=platform_select
    data={platform_options}
    value=platform_id
    label=platform_label
    defaultValue={initialPlatform}
    title="Platform"
    description="OS, CPU architecture and machine type"
/>

<span class="mt-4 text-xs font-medium">Memory: {selected_platform?.[0]?.memory_label ?? 'Unknown'}</span>

<BenchmarkExplorerUrlSync />

```sql geomean
select
  r.benchmark_series,
  r.run_timestamp,
  r.merge_commit_date,
  r.merge_date,
  r.geomean_seconds,
  r.duckdb_version,
  r.duckdb_commit_sha as commit_sha,
  r.duckdb_commit_sha[:8] as commit,
  p.platform_label as platform,
  r.machine_label,
  r.cpu_arch_label,
  r.queries_ok,
  r.queries_attempted,
  r.queries_failed,
  r.is_complete,
  r.queries_sha[:8] as query_set,
  r.os,
  r.queries_sha
from benchmarks.geomean_runs r
join ${platform_options} p
  on p.platform_id = '${inputs.platform_select.value}'
 and p.machine_label = r.machine_label
 and p.cpu_arch_label = r.cpu_arch_label
 and p.os is not distinct from r.os
where r.storage_type = 'ducklake'
  and r.benchmark_series = '${inputs.suite_select.value}'
  and r.merge_commit_date >= '${inputs.date_select.start}'
  and r.merge_commit_date <  '${inputs.date_select.end}'::date + interval 2 day
order by r.merge_commit_date, r.run_timestamp
```

```sql version_baselines
select
  r.benchmark_series,
  r.duckdb_version,
  arg_max(r.geomean_seconds, r.run_timestamp) as baseline_seconds
from benchmarks.geomean_runs r
where r.storage_type = 'ducklake'
  and r.duckdb_version in ('v1.4.5', 'v1.5.5')
  and exists (
    select 1
    from ${geomean} g
    where g.benchmark_series = r.benchmark_series
      and g.machine_label    = r.machine_label
      and g.cpu_arch_label   = r.cpu_arch_label
      and g.os               is not distinct from r.os
      and g.queries_sha      = r.queries_sha
  )
group by r.benchmark_series, r.duckdb_version
order by r.duckdb_version
```

```sql chart_bounds
select max(y) * 1.08 as y_max
from (
  select geomean_seconds as y from ${geomean}
  union all
  select baseline_seconds as y from ${version_baselines}
)
```

```sql query_history
select
  q.run_id,
  q.query,
  q.run_timestamp,
  q.merge_commit_date,
  q.merge_date,
  q.mean_seconds,
  q.median_seconds,
  q.fastest_seconds,
  q.slowest_seconds,
  q.timed_runs,
  q.status,
  q.error,
  q.duckdb_version,
  q.duckdb_commit_sha as commit_sha,
  q.duckdb_commit_sha[:8] as commit,
  q.queries_sha,
  q.queries_sha[:8] as query_set
from benchmarks.query_times q
join ${platform_options} p
  on p.platform_id = '${inputs.platform_select.value}'
 and p.machine_label = q.machine_label
 and p.cpu_arch_label = q.cpu_arch_label
 and p.os is not distinct from q.os
where q.storage_type = 'ducklake'
  and q.benchmark_series = '${inputs.suite_select.value}'
  and q.merge_commit_date >= '${inputs.date_select.start}'
  and q.merge_commit_date <  '${inputs.date_select.end}'::date + interval 2 day
order by q.query, q.merge_commit_date, q.run_timestamp
```

```sql query_baselines
select
  q.query,
  q.duckdb_version,
  arg_max(q.mean_seconds, q.run_timestamp) as baseline_seconds
from benchmarks.query_times q
join ${platform_options} p
  on p.platform_id = '${inputs.platform_select.value}'
 and p.machine_label = q.machine_label
 and p.cpu_arch_label = q.cpu_arch_label
 and p.os is not distinct from q.os
where q.storage_type = 'ducklake'
  and q.benchmark_series = '${inputs.suite_select.value}'
  and q.duckdb_version in ('v1.4.5', 'v1.5.5')
  and q.status = 'ok'
  and q.mean_seconds is not null
  and exists (
    select 1
    from ${query_history} h
    where h.query = q.query
      and h.queries_sha = q.queries_sha
  )
group by q.query, q.duckdb_version
order by q.query, q.duckdb_version
```

```sql history_summary
select
  count(distinct query) as query_count,
  count(distinct run_id) as run_count,
  count(*) filter (where status <> 'ok' or mean_seconds is null) as failed_count
from ${query_history}
```

{#if query_history.dataLoaded && query_history.length === 0}
<Alert status="warning">
No query runs match this suite, platform and date range.
</Alert>
{/if}

## Geometric mean

<BenchmarkSuiteChart
    data={geomean}
    baselines={version_baselines}
    bounds={chart_bounds}
    series={inputs.suite_select.value}
/>

## Query performance over time

{#if history_summary?.[0]}
<p>{history_summary[0].query_count} queries across {history_summary[0].run_count} runs.</p>
{#if history_summary[0].failed_count > 0}
<p>The red × markers show {history_summary[0].failed_count} failed query executions that were
excluded from their run's geomean.</p>
{/if}
{/if}

<BenchmarkQueryGrid
    data={query_history}
    baselines={query_baselines}
    dateStart={inputs.date_select.start}
    dateEnd={inputs.date_select.end}
/>
