---
title: DuckLake Benchmark Query History
sidebar_title: Query History
sidebar_link: false
full_width: true
hide_toc: true
---

<script>
	import { page as pageStore } from '$app/stores';
	import { browser as isBrowser } from '$app/environment';

  import { createBenchmarkDisplayQuery, initialBenchmarkSuite } from '$lib/benchmarkSuiteUtils.js';
  import { Query as BrowserQuery } from '@evidence-dev/sdk/usql';
  import { getQueryFunction } from '@evidence-dev/component-utilities/buildQuery';

  const executeQuery = getQueryFunction();
  const queryForDisplay = createBenchmarkDisplayQuery((text, options) =>
    BrowserQuery.create(text, executeQuery, options));

  $: refreshDisplay = isBrowser && Boolean(suiteParam);
  $: displayGeomean = queryForDisplay(geomean, refreshDisplay);
  $: displayVersionBaselines = queryForDisplay(version_baselines, refreshDisplay);
  $: displayRunFailures = queryForDisplay(run_failures, refreshDisplay);
  $: displayHistory = queryForDisplay(query_history, refreshDisplay);
  $: displayQueryBaselines = queryForDisplay(query_baselines, refreshDisplay);
  $: displaySummary = queryForDisplay(history_summary, refreshDisplay);
  const validDate = (value) => /^\d{4}-\d{2}-\d{2}$/.test(value ?? '')
    && !Number.isNaN(new Date(`${value}T00:00:00Z`).getTime());
  const validPlatform = (value) => /^[a-z0-9._-]+\|[a-z0-9._-]+\|[a-z0-9._-]+$/i.test(value ?? '');
  const validVersion = (value) => /^v\d+\.\d+$/.test(value ?? '');

	$: suiteParam = isBrowser ? $pageStore.url.searchParams.get('suite') : null;
	$: platformParam = isBrowser ? $pageStore.url.searchParams.get('platform') : null;
	$: startParam = isBrowser ? $pageStore.url.searchParams.get('start') : null;
	$: endParam = isBrowser ? $pageStore.url.searchParams.get('end') : null;
	$: versionParam = isBrowser ? $pageStore.url.searchParams.get('version') : null;
  $: initialSuite = initialBenchmarkSuite(suiteParam, suite_options ?? []);
  $: initialPlatform = validPlatform(platformParam)
    ? platformParam
    : 'linux|x86_64|c6id.4xlarge';
  $: validWindow = validDate(startParam) && validDate(endParam) && startParam <= endParam;
  $: initialStart = validWindow ? startParam : undefined;
  $: initialEnd = validWindow ? endParam : new Date();
  $: initialVersion = validVersion(versionParam) ? versionParam : undefined;
</script>

```sql suite_options
select benchmark_series, suite_label
from benchmarks.benchmark_series_list
where storage_type = 'ducklake'
order by suite_label
```

```sql date_options
select merge_commit_date::date as merge_commit_date
from benchmarks.query_times
where storage_type = 'ducklake'
  and merge_commit_date is not null
```

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

```sql version_options
-- The release lines that have plottable runs, newest first. The first is selected by default.
select
  'v' || regexp_extract(duckdb_version, '^v?([0-9]+\.[0-9]+)', 1) as version_line,
  regexp_extract(duckdb_version, '^v?([0-9]+)\.', 1)::int as major,
  regexp_extract(duckdb_version, '^v?[0-9]+\.([0-9]+)', 1)::int as minor
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and merge_commit_date is not null
group by all
order by major desc, minor desc
```

<div class="flex flex-wrap items-end gap-x-3">
  <div>
    <ButtonGroup
        name=version_select
        data={version_options}
        value=version_line
        defaultValue={initialVersion ?? version_options?.[0]?.version_line}
    />
  </div>

  <div>
    <DateRange
        name=date_select
        data={date_options}
        dates=merge_commit_date
        start={initialStart}
        end={initialEnd}
        defaultValue={validWindow ? undefined : 'Last 90 Days'}
    />
  </div>

  <div class="min-w-[15rem]">
    <Dropdown
        name=suite_select
        data={suite_options}
        value=benchmark_series
        label=suite_label
        defaultValue={initialSuite}
        title="Benchmark suite"
    />
  </div>
</div>

<div class="flex flex-wrap items-end gap-x-3">
  <div class="flex min-w-[20rem] items-end gap-2">
    <Dropdown
        name=platform_select
        data={platform_options}
        value=platform_id
        label=platform_label
        defaultValue={initialPlatform}
        title="Platform"
        description="OS, CPU architecture and machine type"
    />
    <span class="mb-4">
      <BenchmarkMemoryBadge memory={selected_platform?.[0]?.memory_label} />
    </span>
  </div>
</div>

<BenchmarkExplorerUrlSync />

```sql geomean
select
  r.run_id,
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
  -- only runs of the selected release line
  and 'v' || regexp_extract(r.duckdb_version, '^v?([0-9]+\.[0-9]+)', 1) = '${inputs.version_select}'
order by r.merge_commit_date, r.run_timestamp
```

```sql version_baselines
select
  r.benchmark_series,
  r.duckdb_version,
  arg_max(r.geomean_seconds, r.run_timestamp) as baseline_seconds
from benchmarks.geomean_runs r
where r.storage_type = 'ducklake'
  and regexp_full_match(r.duckdb_version, 'v?[0-9]+\.[0-9]+\.[0-9]+')
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

```sql run_failures
-- Which queries failed in each plotted run, listed in the tooltip of an incomplete point.
select
  q.run_id,
  string_agg(q.query, ', ' order by q.query) as failed_queries
from benchmarks.query_times q
where q.storage_type = 'ducklake'
  and q.run_id in (select run_id from ${geomean})
  and q.status <> 'ok'
group by q.run_id
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
  -- only runs of the selected release line
  and 'v' || regexp_extract(q.duckdb_version, '^v?([0-9]+\.[0-9]+)', 1) = '${inputs.version_select}'
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
  and regexp_full_match(q.duckdb_version, 'v?[0-9]+\.[0-9]+\.[0-9]+')
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
  count(*) filter (where status <> 'ok') as failed_count
from ${query_history}
```

{#if $displayHistory.dataLoaded && $displayHistory.length === 0}
<Alert status="warning">
No query runs match this suite, platform and date range.
</Alert>
{/if}

<div id="geometric-mean" class="scroll-mt-[6.25rem]">
  <h2 class="markdown"><a href="#geometric-mean">Geometric mean</a></h2>
  <BenchmarkSuiteChart
      data={$displayGeomean}
      baselines={$displayVersionBaselines}
      failures={$displayRunFailures}
      series={inputs.suite_select.value}
      dateStart={inputs.date_select.start}
      dateEnd={inputs.date_select.end}
  />
</div>

<BenchmarkFailureHistory data={$displayHistory} />

<BenchmarkQueryHistoryHeading
    suite={inputs.suite_select.value}
    dateStart={inputs.date_select.start}
    dateEnd={inputs.date_select.end}
/>

{#if $displaySummary?.[0]?.failed_count > 0}
<p>The red × markers show the commit dates of {$displaySummary[0].failed_count} failed query
executions, which have no timing and were excluded from their run's geomean. Expand the failure
history below the geometric mean chart to see each run's failed queries and errors. A red badge indicates that its latest run in the
selected window failed.</p>
{/if}

<BenchmarkQueryGrid
    data={$displayHistory}
    baselines={$displayQueryBaselines}
    dateStart={inputs.date_select.start}
    dateEnd={inputs.date_select.end}
/>
