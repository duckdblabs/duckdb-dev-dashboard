---
title: Benchmarks - DuckDB storage
---

Geometric mean of query execution time on **DuckDB** storage, over time.
Each storage backend is on its own page.

Each point is one benchmark run: the mean of its warm runs per query, then the geometric mean
across the benchmark's queries. Data comes from the benchmark results lake written by
`scripts/engineering/benchmark` in `duckdb-internal`. Test runs (`is_test`) are excluded.

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
where storage_type = 'duckdb'
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
where storage_type = 'duckdb'
  and benchmark in ${inputs.benchmark_select.value}
  -- the scale-factor filter only bites on benchmarks that have one; clickbench (scale_factor
  -- NULL) is exempt, so narrowing to sf100 does not make it disappear
  and (scale_factor is null or scale_factor_label in ${inputs.sf_select.value})
  and machine_label in ${inputs.machine_select.value}
  and cpu_arch_label in ${inputs.cpu_arch_select.value}
  and run_timestamp between '${inputs.date_select.start}' and '${inputs.date_select.end}'
order by run_timestamp
```

```sql version_baselines
-- One baseline per pinned duckdb version per chart, drawn as a reference line.
--
-- Pinned deliberately rather than derived: these are the two releases currently worth comparing
-- against, and a new release should not start drawing a line until someone decides it should.
-- Add to this list to add a reference line.
--
-- Deliberately NOT filtered by the date range: a baseline is a fixed point of comparison, and
-- narrowing the window should not make it vanish. The releases were measured well before most of
-- the alpha runs.
select
  benchmark_series,
  duckdb_version,
  -- the latest run of that version
  arg_max(geomean_seconds, run_timestamp) as baseline_seconds
from benchmarks.geomean_runs
where storage_type = 'duckdb'
  and duckdb_version in ('v1.4.5', 'v1.5.5')
  and benchmark in ${inputs.benchmark_select.value}
  and (scale_factor is null or scale_factor_label in ${inputs.sf_select.value})
  and machine_label in ${inputs.machine_select.value}
  and cpu_arch_label in ${inputs.cpu_arch_select.value}
group by benchmark_series, duckdb_version
order by benchmark_series, duckdb_version
```

```sql chart_bounds
-- A little headroom above each chart's tallest element.
--
-- Needed because a reference line exactly at the chart maximum is drawn on the plot border and is
-- indistinguishable from it.
select
  benchmark_series,
  max(y) * 1.08 as y_max
from (
  select benchmark_series, geomean_seconds  as y from ${geomean}
  union all
  select benchmark_series, baseline_seconds as y from ${version_baselines}
)
group by benchmark_series
```

```sql series_shown
select distinct benchmark_series
from ${geomean}
order by benchmark_series
```

## Geometric mean per benchmark

One chart per benchmark and scale factor.

Each dot is one run concerning a different commit.

Dashed lines mark what duckdb v1.4.5 and v1.5.5 achieved on that benchmark, so the ongoing
`v2.0.0-alpha` series can be read against them. A version with no run for a given benchmark simply
has no line there.

<!--
  sort=false keeps the points in the order the geomean query returns them (by run_timestamp).
  Evidence's default sort=true reorders the rows by the *y* value, descending, whenever the x
  column is a string - and run_date is a varchar - which scrambles the dates along the category
  axis.
-->
{#each series_shown as s}
  <LineChart
      data={geomean.filter(d => d.benchmark_series === s.benchmark_series)}
      x=run_date
      xType=category
      showAllXAxisLabels=false
      y=geomean_seconds
      yMax={chart_bounds.find(b => b.benchmark_series === s.benchmark_series)?.y_max}
      title={s.benchmark_series}
      yAxisTitle="geomean (seconds)"
      markers=true
      lineWidth=0
      sort=false
  >
      <!--
        Back to a single data-driven line now that every label uses the same position: the split
        into one component per baseline existed only so the label ends could alternate.
        hideValue drops the ' (0.0929)' suffix the component appends by default - the value is
        readable off the y-axis, and the version is what identifies the line.
        emptySet=pass so a release with no run for this benchmark draws nothing instead of warning
        (v1.4.5 has no DuckLake runs).
      -->
      <ReferenceLine
          data={version_baselines.filter(d => d.benchmark_series === s.benchmark_series)}
          y=baseline_seconds
          label=duckdb_version
          hideValue=true
          lineType=dashed
          labelPosition=belowEnd
          emptySet=pass
      />
  </LineChart>
{/each}

## Runs

A run with failed queries is still plotted, but its geomean covers fewer queries than a complete
run - `# failed` is what tells them apart.

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

## Per-query execution times

The individual queries of a single run, each against the two release baselines. Every timing is a
median over that query's warm runs.

`ratio vs ...` is the selected run divided by the baseline: **above 1.0 means the selected run is
slower** than that release, below 1.0 means faster. Rows are sorted by the v1.5.5 ratio, so
regressions are at the top and improvements at the bottom. A blank baseline means that release has
no run of this query - v1.4.5 has no DuckLake runs at all. Failed queries are listed with empty
timings.

```sql run_options
select
  run_id,
  run_date || '  -  ' || benchmark_series || '  -  ' || duckdb_version as run_label,
  run_timestamp
from benchmarks.geomean_runs
where storage_type = 'duckdb'
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
-- The selected run's queries, each next to the v1.5.5 and v1.4.5 medians for the same query.
--
-- Baselines are joined on (benchmark_series, query), not query alone: tpch and tpcds both have a
-- q01, and matching on the query name by itself would compare unrelated queries.
with selected as (
  select *
  from benchmarks.query_times
  where storage_type = 'duckdb'
    -- fall back to the newest run in run_options when nothing is selected yet, so the table is
    -- never empty on first load. The nullifs cover an input that is unset rather than chosen.
    and run_id = coalesce(
          nullif(nullif('${inputs.run_select.value}', ''), 'undefined'),
          (select run_id from ${run_options} order by run_timestamp desc limit 1))
),
baselines as (
  select
    benchmark_series,
    query,
    duckdb_version,
    median(median_seconds) as baseline_seconds
  from benchmarks.query_times
  where storage_type = 'duckdb'
    and duckdb_version in ('v1.4.5', 'v1.5.5')
  group by benchmark_series, query, duckdb_version
)
select
  s.query,
  round(s.median_seconds, 4)                        as 'median (s)',
  round(b55.baseline_seconds, 4)                    as 'v1.5.5 (s)',
  round(b45.baseline_seconds, 4)                    as 'v1.4.5 (s)',
  round(s.median_seconds / nullif(b55.baseline_seconds, 0), 3) as 'ratio vs v1.5.5',
  round(s.median_seconds / nullif(b45.baseline_seconds, 0), 3) as 'ratio vs v1.4.5',
  s.timed_runs                                      as '# warm runs',
  s.status
from selected s
-- left joins: a release with no run of this query leaves the baseline and its ratio blank
-- rather than dropping the query from the table
left join baselines b55
  on  b55.benchmark_series = s.benchmark_series
  and b55.query            = s.query
  and b55.duckdb_version   = 'v1.5.5'
left join baselines b45
  on  b45.benchmark_series = s.benchmark_series
  and b45.query            = s.query
  and b45.duckdb_version   = 'v1.4.5'
-- regressions first: biggest ratio at the top, improvements at the bottom. Ordered on
-- the v1.5.5 ratio (the newer release) and falling back to v1.4.5 where v1.5.5 has no run of the
-- query; a query with neither baseline sorts last rather than to the top as a NULL.
order by coalesce("ratio vs v1.5.5", "ratio vs v1.4.5") desc nulls last,
         s.median_seconds desc
```

<DataTable data={query_times} rows=20 search=true>
    <Column id=query />
    <Column id='median (s)' />
    <Column id='v1.5.5 (s)' />
    <Column id='v1.4.5 (s)' />
    <Column id='ratio vs v1.5.5' />
    <Column id='ratio vs v1.4.5' />
    <Column id='# warm runs' />
    <Column id=status />
</DataTable>
