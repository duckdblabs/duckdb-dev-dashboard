---
title: Benchmarks - DuckLake storage
---

Geometric mean of query execution time on **DuckLake** storage, over time.

Each point is one benchmark run: the mean of its warm runs per query, then the geometric mean
across the benchmark's queries. Data comes from the benchmark results lake written by
`scripts/engineering/benchmark` in `duckdb-internal`. Test runs (`is_test`) are excluded.

Other storage backends are on their own pages, so a slower backend never rescales a chart it does
not belong to.

## Runs

A run with failed queries is still plotted, but its geomean covers fewer queries than a complete
run - `# failed` is what tells them apart.

<DataTable data={run_table} rows=25 search=true>
    <Column id=benchmark_series />
    <Column id=run_date />
    <Column id=duckdb_version />
    <Column id=commit />
    <Column id='geomean (s)' />
    <Column id='# ok' />
    <Column id='# failed' />
    <Column id=machine_label />
    <Column id=cpu_arch_label />
    <Column id=query_set />
</DataTable>

```sql benchmark_options
select benchmark from benchmarks.benchmark_list
```

```sql sf_options
select scale_factor_label from benchmarks.scale_factor_list
```

```sql machine_options
select machine_label from benchmarks.machine_list
```

```sql cpu_arch_options
select cpu_arch_label from benchmarks.cpu_arch_list
```

### Filters

<DateRange
    name=date_select
    defaultValue={'Last 90 Days'}
    title="Select time window"
    description="Select time window"
/>
<br>
<Dropdown
    name=benchmark_select
    data={benchmark_options}
    value=benchmark
    selectAllByDefault=true
    multiple=true
    title="Select benchmark"
    description="Select benchmark suite"
/>

```sql sf_applicable
select count(*) as n
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and benchmark in ${inputs.benchmark_select.value}
  and scale_factor is not null
```

<!--
  The scale-factor filter is hidden when the benchmark selection contains nothing that has a
  scale factor - i.e. clickbench only. Hidden with CSS rather than removed with an if-block on
  purpose: unmounting the Dropdown drops sf_select from the inputs store, while the geomean query
  below still interpolates it, which would break the whole page instead of hiding one control.
-->
<div style="display: {(sf_applicable?.[0]?.n ?? 0) > 0 ? 'block' : 'none'}">
<br>
<Dropdown
    name=sf_select
    data={sf_options}
    value=scale_factor_label
    selectAllByDefault=true
    multiple=true
    title="Select scale factor (tpch / tpcds)"
    description="Only applies to benchmarks that have a scale factor; clickbench is always shown"
/>
</div>
<br>
<Dropdown
    name=machine_select
    data={machine_options}
    value=machine_label
    selectAllByDefault=true
    multiple=true
    title="Select machine type"
    description="Select machine type"
/>
<br>
<Dropdown
    name=cpu_arch_select
    data={cpu_arch_options}
    value=cpu_arch_label
    selectAllByDefault=true
    multiple=true
    title="Select CPU architecture"
    description="Timings from different CPU architectures are not comparable"
/>

```sql geomean
select
  benchmark_series,
  benchmark,
  scale_factor_label,
  run_timestamp,
  run_date,
  geomean_seconds,
  duckdb_version,
  duckdb_commit_sha[:8] as commit,
  machine_label,
  cpu_arch_label,
  queries_ok,
  queries_attempted,
  queries_failed,
  is_complete,
  queries_sha[:8] as query_set
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and benchmark in ${inputs.benchmark_select.value}
  -- the scale-factor filter only bites on benchmarks that have one; clickbench (scale_factor
  -- NULL) is exempt, so narrowing to sf100 does not make it disappear
  and (scale_factor is null or scale_factor_label in ${inputs.sf_select.value})
  and machine_label in ${inputs.machine_select.value}
  and cpu_arch_label in ${inputs.cpu_arch_select.value}
  and run_timestamp between '${inputs.date_select.start}' and '${inputs.date_select.end}'
order by run_timestamp
```

```sql series_shown
select distinct benchmark_series
from ${geomean}
order by benchmark_series
```

## Geometric mean per benchmark

One chart per benchmark and scale factor. The benchmarks span orders of magnitude, so they do not
share an axis.

<Grid cols=2>
{#each series_shown as s}
  <LineChart
      data={geomean.filter(d => d.benchmark_series === s.benchmark_series)}
      x=run_timestamp
      y=geomean_seconds
      title={s.benchmark_series}
      yAxisTitle="geomean (seconds)"
      markers=true
      handleMissing=connect
  />
{/each}
</Grid>

<!--
  This query stays below the filters even though its table renders at the top of the page: it
  interpolates the filter inputs, which do not exist until their components are declared.
  A sql block renders nothing itself, so its position does not affect the layout.
-->
```sql run_table
select
  benchmark_series,
  run_date,
  duckdb_version,
  commit,
  round(geomean_seconds, 4) as 'geomean (s)',
  queries_ok as '# ok',
  queries_failed as '# failed',
  machine_label,
  cpu_arch_label,
  query_set
from ${geomean}
order by run_timestamp desc
```


## Per-query execution times

The individual queries of a single run. `median (s)` and `mean (s)` are both computed over that
query's warm runs: `mean` is what feeds the geomean above, and a gap between the two means those
warm runs were noisy. `spread (s)` is slowest minus fastest warm run. Failed queries are listed
with empty timings.

```sql run_options
select
  run_id,
  run_date || '  -  ' || benchmark_series || '  -  ' || duckdb_version as run_label,
  run_timestamp
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and benchmark in ${inputs.benchmark_select.value}
  and (scale_factor is null or scale_factor_label in ${inputs.sf_select.value})
  and machine_label in ${inputs.machine_select.value}
  and cpu_arch_label in ${inputs.cpu_arch_select.value}
  and run_timestamp between '${inputs.date_select.start}' and '${inputs.date_select.end}'
order by run_timestamp desc
```

<Dropdown
    name=run_select
    data={run_options}
    value=run_id
    label=run_label
    defaultValue={run_options?.[0]?.run_id}
    title="Select run"
    description="One benchmark at one point in time; defaults to the most recent run matching the filters above"
/>

```sql query_times
select
  query,
  round(median_seconds, 4) as 'median (s)',
  round(mean_seconds, 4)   as 'mean (s)',
  round(slowest_seconds - fastest_seconds, 4) as 'spread (s)',
  timed_runs as '# warm runs',
  status
from benchmarks.query_times
where storage_type = 'ducklake'
  -- fall back to the newest run in run_options when nothing is selected yet, so the table is
  -- never empty on first load. The nullifs cover an input that is unset rather than chosen.
  and run_id = coalesce(
        nullif(nullif('${inputs.run_select.value}', ''), 'undefined'),
        (select run_id from ${run_options} order by run_timestamp desc limit 1))
order by median_seconds desc nulls last
```

<DataTable data={query_times} rows=20 search=true>
    <Column id=query />
    <Column id='median (s)' />
    <Column id='mean (s)' />
    <Column id='spread (s)' />
    <Column id='# warm runs' />
    <Column id=status />
</DataTable>
