---
title: Benchmarks - DuckLake storage
---

Geometric mean of query execution time on **DuckLake** storage, over time.

Each point is one benchmark run: the mean of its warm runs per query, then the geometric mean
across the benchmark's queries. Data comes from the benchmark results lake written by
`scripts/engineering/benchmark` in `duckdb-internal`. Test runs (`is_test`) are excluded.

Other storage backends are on their own pages, so a slower backend never rescales a chart it does
not belong to.

The filters in the **Filters** section below control everything on this page: the charts and
their baseline lines, the runs table, and the per-query execution times.

```sql benchmark_options
select benchmark from benchmarks.benchmark_list
```

```sql sf_options
select scale_factor_label from benchmarks.scale_factor_list
```

```sql machine_options
-- only machines this page has runs with a merge date for (the date filter drops the rest): the
-- filter is single-select, so an option without runs would empty the whole page
select distinct machine_label
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and merge_commit_date is not null
order by machine_label
```

```sql cpu_arch_options
-- scoped like machine_options, for the same reason
select distinct cpu_arch_label
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and merge_commit_date is not null
order by cpu_arch_label
```

```sql date_options
-- bounds the date picker to the merge dates of this page's runs, so the presets ('Last 90 Days')
-- count back from the newest benchmarked commit rather than from today
select merge_commit_date::date as merge_commit_date
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and merge_commit_date is not null
```

### Filters

<DateRange
    name=date_select
    data={date_options}
    dates=merge_commit_date
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
<!--
  Single-select, so the charts never mix timings from different hardware. A ButtonGroup selects
  nothing without a defaultValue - and every query would then match no runs - so both are pinned
  to the only machine with DuckLake runs that have a merge date.
-->
<ButtonGroup
    name=machine_select
    data={machine_options}
    value=machine_label
    defaultValue="unspecified"
    title="Select machine type"
    description="Timings from different machines are not comparable"
/>
<br>
<ButtonGroup
    name=cpu_arch_select
    data={cpu_arch_options}
    value=cpu_arch_label
    defaultValue="x86_64"
    title="Select CPU architecture"
    description="Timings from different CPU architectures are not comparable"
/>

<!-- dataLoaded: without it the warning flashes while the query is still running -->
{#if geomean.dataLoaded && geomean.length === 0}
<Alert status="warning">
No runs match these filters. Each machine has a single CPU architecture, so a machine paired with
a different architecture leaves nothing to show.
</Alert>
{/if}

```sql geomean
select
  benchmark_series,
  benchmark,
  scale_factor_label,
  run_timestamp,
  merge_commit_date,
  merge_date,
  geomean_seconds,
  duckdb_version,
  duckdb_commit_sha[:8] as commit,
  machine_label,
  cpu_arch_label,
  queries_ok,
  queries_attempted,
  queries_failed,
  is_complete,
  queries_sha[:8] as query_set,
  -- not displayed: version_baselines matches release runs to the plotted runs on these
  os,
  queries_sha
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and benchmark in ${inputs.benchmark_select.value}
  -- the scale-factor filter only bites on benchmarks that have one; clickbench (scale_factor
  -- NULL) is exempt, so narrowing to sf100 does not make it disappear
  and (scale_factor is null or scale_factor_label in ${inputs.sf_select.value})
  and machine_label = '${inputs.machine_select}'
  and cpu_arch_label = '${inputs.cpu_arch_select}'
  -- the date filter is on the commit's merge date, not on when it was benchmarked. Release runs
  -- have no merge date and are deliberately excluded; they remain as the baselines below.
  --
  -- The end bound is two days past the input on purpose. The picker emits bare dates, converted
  -- via UTC, so east of UTC it reports each day as the day before - picking Sep 3 sends
  -- '2026-09-02'. One day compensates for that, the other makes the end day inclusive. The window
  -- can come out a day wider than the picker shows, but never narrower; with the picker bounded
  -- to the newest merge date, narrower would drop exactly the newest commit.
  and merge_commit_date >= '${inputs.date_select.start}'
  and merge_commit_date <  '${inputs.date_select.end}'::date + interval 2 day
order by merge_commit_date, run_timestamp
```

```sql version_baselines
-- One baseline per pinned duckdb version per chart, drawn as a reference line.
--
-- Pinned deliberately rather than derived: these are the two releases currently worth comparing
-- against, and a new release should not start drawing a line until someone decides it should.
-- Add to this list to add a reference line.
--
-- Only release runs comparable to a run plotted in the same chart count: same machine, CPU, OS
-- and query set (queries_sha). Anything else is a different measurement - on the DuckDB page, the
-- c6id release runs of 2026-09-09 were on Windows with an edited query set, and they put the lines
-- at up to 2x the Linux runs they sat next to.
--
-- The median over those runs rather than the latest one, so a single unusual run cannot move the
-- line.
--
-- Deliberately NOT filtered by the date range: a baseline is a fixed point of comparison, and
-- narrowing the window should not make it vanish. The releases were measured well before most of
-- the alpha runs. The window only decides which OS and query sets are on the chart to match.
select
  r.benchmark_series,
  r.duckdb_version,
  median(r.geomean_seconds) as baseline_seconds
from benchmarks.geomean_runs r
where r.storage_type = 'ducklake'
  and r.duckdb_version in ('v1.4.5', 'v1.5.5')
  -- the geomean query already carries the benchmark, scale-factor, machine and CPU filters, so
  -- matching a row of it applies them here too
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
order by r.benchmark_series, r.duckdb_version
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

One chart per benchmark and scale factor. The benchmarks span orders of magnitude, so they do not
share an axis.

Each dot is one run, placed at the date its commit was merged; runs of the same commit stack on the
same date. They are deliberately not connected: consecutive runs are different commits,
not a continuous measurement, so a line between them would imply a trend that the data does not
support.

Dashed lines mark what duckdb v1.4.5 and v1.5.5 achieved on that benchmark, so the ongoing
`v2.0.0-alpha` series can be read against them. Each line is the median of that release's runs on
the same machine, OS and query set as the runs in the chart. A version with no such run simply has
no line there.

<!--
  sort=false keeps the points in the order the geomean query returns them (by merge_commit_date).
  Evidence's default sort=true reorders the rows by the *y* value, descending, whenever the x
  column is a string - and merge_date is a varchar - which scrambles the dates along the category
  axis.
-->
<script>
  // x-axis labels as 'Aug 17, 17:10'. Display only: merge_date itself stays the category, so
  // commits merged on the same day keep separate x positions, and the tooltip keeps the full
  // timestamp. The time is in the label so that two commits merged on the same day do not both
  // read 'Aug 17'.
  // Read from the string rather than through Date, which would shift it into the viewer's
  // timezone - merge_date is UTC.
  const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const monthDayTime = (v) => {
    const m = /^\d{4}-(\d{2})-(\d{2}) (\d{2}:\d{2})/.exec(String(v));
    return m ? `${MONTHS[Number(m[1]) - 1]} ${Number(m[2])}, ${m[3]}` : v;
  };
</script>

{#each series_shown as s}
  <LineChart
      data={geomean.filter(d => d.benchmark_series === s.benchmark_series)}
      x=merge_date
      xType=category
      showAllXAxisLabels=false
      echartsOptions={{ xAxis: { axisLabel: { formatter: monthDayTime } } }}
      y=geomean_seconds
      yMax={chart_bounds.find(b => b.benchmark_series === s.benchmark_series)?.y_max}
      title={s.benchmark_series}
      yAxisTitle="geomean (seconds)"
      markers=true
      lineWidth=0
      sort=false
  >
      <!--
        One component per release, so each gets its own colour and label side. The two releases
        land within a few pixels of each other on most charts, and with one shared belowEnd
        position the later label covered the earlier one completely - Evidence does not move
        overlapping reference labels apart.
        v1.4.5 is labelled above its line and v1.5.5 below: v1.5.5 is the faster of the two on
        every chart so far, so the labels move apart. Should that ever flip, they would move
        towards each other again, and the colours are what still tells the lines apart.
        Each colour is a [light, dark] pair so it stays readable under the theme switcher (label
        contrast >= 5:1 on both backgrounds); orange and teal also stay distinct from each other
        under red-green colour blindness.
        hideValue drops the ' (0.0929)' suffix the component appends by default - the value is
        readable off the y-axis, and the version is what identifies the line.
        emptySet=pass so a release with no matching run for this chart draws nothing instead of
        warning.
      -->
      <ReferenceLine
          data={version_baselines.filter(d => d.benchmark_series === s.benchmark_series && d.duckdb_version === 'v1.4.5')}
          y=baseline_seconds
          label=duckdb_version
          hideValue=true
          lineType=dashed
          color={['#c2410c', '#fb923c']}
          labelPosition=aboveEnd
          emptySet=pass
      />
      <ReferenceLine
          data={version_baselines.filter(d => d.benchmark_series === s.benchmark_series && d.duckdb_version === 'v1.5.5')}
          y=baseline_seconds
          label=duckdb_version
          hideValue=true
          lineType=dashed
          color={['#0f766e', '#2dd4bf']}
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
  merge_date,
  duckdb_version,
  commit,
  round(geomean_seconds, 4) as 'geomean (s)',
  queries_ok as '# ok',
  queries_failed as '# failed',
  machine_label,
  cpu_arch_label,
  query_set
from ${geomean}
order by merge_commit_date desc, run_timestamp desc
```

<DataTable data={run_table} rows=25 search=true>
    <Column id=benchmark_series />
    <Column id=merge_date />
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

The individual queries of a single run, each against the two release baselines on the selected
machine type and CPU architecture. Every timing of the release baselines is a median over that query's warm runs.

`ratio vs ...` is the selected run divided by the baseline: **above 1.0 means the selected run is
slower** than that release, below 1.0 means faster. A ratio above 1.1 is shaded red and one below
0.9 green; the band in between is left uncoloured. Rows are sorted by the
v1.5.5 ratio, so regressions are at the top and improvements at the bottom. A blank baseline means
that release has no run of this query - v1.4.5 has no DuckLake runs at all. Failed queries are
listed with empty timings.

```sql run_options
select
  run_id,
  run_date || '  -  ' || benchmark_series || '  -  ' || duckdb_version as run_label,
  run_timestamp
from benchmarks.geomean_runs
where storage_type = 'ducklake'
  and benchmark in ${inputs.benchmark_select.value}
  and (scale_factor is null or scale_factor_label in ${inputs.sf_select.value})
  and machine_label = '${inputs.machine_select}'
  and cpu_arch_label = '${inputs.cpu_arch_select}'
  -- same date filter as the geomean query above - see there for the two-day end bound
  and merge_commit_date >= '${inputs.date_select.start}'
  and merge_commit_date <  '${inputs.date_select.end}'::date + interval 2 day
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
  where storage_type = 'ducklake'
    and run_id = coalesce(
          (select run_id from ${run_options} where run_id = '${inputs.run_select.value}'),
          (select run_id from ${run_options} order by run_timestamp desc limit 1))
),
baselines as (
  select
    benchmark_series,
    query,
    duckdb_version,
    median(median_seconds) as baseline_seconds
  from benchmarks.query_times
  where storage_type = 'ducklake'
    and duckdb_version in ('v1.4.5', 'v1.5.5')
    and machine_label  = '${inputs.machine_select}'
    and cpu_arch_label = '${inputs.cpu_arch_select}'
  group by benchmark_series, query, duckdb_version
)
select
  s.query,
  round(s.median_seconds, 4)                        as 'median (s)',
  round(b55.baseline_seconds, 4)                    as 'v1.5.5 (s)',
  round(b45.baseline_seconds, 4)                    as 'v1.4.5 (s)',
  -- Rounded to the 2 decimals the table actually displays, not to 3.
  --
  -- The flags below compare against this same rounded value, so the colour can never disagree
  -- with the number in the cell. At 3 decimals it did: 1.101 and 1.100 both print as "1.10" but
  -- only the first is > 1.1, so one 1.10 came out red and the next did not.
  round(s.median_seconds / nullif(b55.baseline_seconds, 0), 2) as 'ratio vs v1.5.5',
  round(s.median_seconds / nullif(b45.baseline_seconds, 0), 2) as 'ratio vs v1.4.5',
  -- Colour flags for the two ratio columns, not shown in the table themselves: -1 paints the
  -- cell green, 1 paints it red, NULL leaves it with the plain cell background.
  --
  -- NULL deliberately covers two different cases at once - a ratio inside the neutral 0.9-1.1
  -- band, and a query the release has no run of. Both should render uncoloured, and a colorscale
  -- maps a null scale value to the table background, so one NULL expresses both.
  --
  -- Flags rather than colouring on the ratio itself because a colorscale interpolates: scaling on
  -- the raw ratio would tint everything between 0.9 and 1.1 some shade of pink or green instead
  -- of leaving it alone.
  case when "ratio vs v1.5.5" > 1.1 then 1 when "ratio vs v1.5.5" < 0.9 then -1 end as 'v1.5.5 color',
  case when "ratio vs v1.4.5" > 1.1 then 1 when "ratio vs v1.4.5" < 0.9 then -1 end as 'v1.4.5 color',
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

<!--
  The two ratio columns are coloured off their companion `... color` flag column rather than off
  their own value - see the query above for why.

  Each colorScale entry is a `[light appearance, dark appearance]` pair, so the tints follow the
  theme switcher rather than being pinned to one appearance - a light wash that reads on white
  would be a glare on the dark background, and vice versa. Order matches colorBreakpoints: green
  for the -1 flag, red for +1. They are deliberately washed out; the table is read by scanning the
  numbers, and the fill is only there to say where to look.

  colorMin/colorMax are set explicitly because the component otherwise derives the scale domain
  from the flag column and gives up when every flag happens to be identical - which is exactly the
  run where every query regressed, i.e. the one that most needs colouring.

  fmt=num2 pins the display to the 2 decimals the query rounds to. Left to itself the table picks
  the decimal count from the column's median, so a run where most queries got faster (median below
  1) would start showing 3 - and the extra digit would be a rounding artefact, not a measurement.

  data is the query object itself, deliberately: search=true pushes the search down into SQL and
  is silently dropped if it is handed plain rows instead.
-->
<DataTable data={query_times} rows=20 search=true>
    <Column id=query />
    <Column id='median (s)' />
    <Column id='v1.5.5 (s)' />
    <Column id='v1.4.5 (s)' />
    <Column
        id='ratio vs v1.5.5'
        fmt=num2
        contentType=colorscale
        scaleColumn='v1.5.5 color'
        colorScale={[['#dcfce7', '#14532d'], ['#fee2e2', '#7f1d1d']]}
        colorBreakpoints={[-1, 1]}
        colorMin={-1}
        colorMax={1}
    />
    <Column
        id='ratio vs v1.4.5'
        fmt=num2
        contentType=colorscale
        scaleColumn='v1.4.5 color'
        colorScale={[['#dcfce7', '#14532d'], ['#fee2e2', '#7f1d1d']]}
        colorBreakpoints={[-1, 1]}
        colorMin={-1}
        colorMax={1}
    />
    <Column id='# warm runs' />
    <Column id=status />
</DataTable>
