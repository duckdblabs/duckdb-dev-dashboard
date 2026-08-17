---
title: Benchmarks
---

Geometric mean of query execution time per benchmark, over time.

Each point is one benchmark run: the mean of its warm runs per query, then the geometric mean
across the benchmark's queries. Data comes from the benchmark results lake written by
`scripts/engineering/benchmark` in `duckdb-internal`. Test runs (`is_test`) are excluded.

```sql series_options
select benchmark_series from benchmarks.benchmark_series_list
```

```sql config_options
select benchmark_name from benchmarks.config_list
```

```sql machine_options
select machine_label from benchmarks.machine_list
```

### Filters

<DateRange
    name=date_select
    defaultValue={'Last 12 Months'}
    title="Select time window"
    description="Select time window"
/>
<br>
<Dropdown
    name=config_select
    data={config_options}
    value=benchmark_name
    selectAllByDefault=true
    multiple=true
    title="Select storage backend"
    description="Select storage backend (benchmark_name)"
/>
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

```sql geomean
select
  benchmark_series,
  benchmark,
  benchmark_name,
  scale_factor,
  run_timestamp,
  run_date,
  geomean_seconds,
  duckdb_version,
  duckdb_commit_sha[:8] as commit,
  machine_label,
  queries_ok,
  queries_attempted,
  queries_failed,
  is_complete,
  queries_sha[:8] as query_set
from benchmarks.geomean_runs
where benchmark_name in ${inputs.config_select.value}
  and machine_label in ${inputs.machine_select.value}
  and run_timestamp between '${inputs.date_select.start}' and '${inputs.date_select.end}'
order by run_timestamp
```

## Geometric mean per benchmark

One chart per benchmark and scale factor; one line per storage backend. The benchmarks span orders
of magnitude, so they do not share an axis.

<Grid cols=2>
{#each series_options as s}
  <LineChart
      data={geomean.filter(d => d.benchmark_series === s.benchmark_series)}
      x=run_timestamp
      y=geomean_seconds
      series=benchmark_name
      title={s.benchmark_series}
      yAxisTitle="geomean (seconds)"
      markers=true
      handleMissing=connect
  />
{/each}
</Grid>

## Runs

A run with failed queries is still plotted, but its geomean covers fewer queries than a complete
run - `# failed` is what tells them apart.

```sql run_table
select
  benchmark_series,
  benchmark_name,
  run_date,
  duckdb_version,
  commit,
  round(geomean_seconds, 4) as 'geomean (s)',
  queries_ok as '# ok',
  queries_failed as '# failed',
  machine_label,
  query_set
from ${geomean}
order by run_timestamp desc
```

<DataTable data={run_table} rows=25 search=true>
    <Column id=benchmark_series />
    <Column id=benchmark_name />
    <Column id=run_date />
    <Column id=duckdb_version />
    <Column id=commit />
    <Column id='geomean (s)' />
    <Column id='# ok' />
    <Column id='# failed' />
    <Column id=machine_label />
    <Column id=query_set />
</DataTable>
