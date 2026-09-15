---
title: Benchmarks on DuckDB Storage
sidebar_title: DuckDB Storage
---

```sql date_options
-- starts the date picker at this page's first benchmarked commit, so 'All Time' does not reach
-- back to 1970. Only the start is taken from here: the DateRange pins the end to today.
select merge_commit_date::date as merge_commit_date
from benchmarks.geomean_runs
where storage_type = 'duckdb'
  and merge_commit_date is not null
```

<DateRange
    name=date_select
    data={date_options}
    dates=merge_commit_date
    end={new Date()}
    defaultValue={'Last 90 Days'}
/>

<br>

```sql platform_options
-- A platform is the combination of OS, CPU architecture and machine type: timings from different
-- combinations are not comparable. The hardware details are curated because the benchmark lake
-- does not record vCPU count, RAM or OS version. Unknown combinations retain a generic label
-- rather than disappearing from the dashboard.
select distinct
  concat_ws('|', coalesce(os, 'unknown'), cpu_arch_label, machine_label) as platform_id,
  os,
  cpu_arch_label,
  machine_label,
  case
    when os = 'linux' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then 'Ubuntu 24.04 (amd64, 16 vCPU)'
    when os = 'linux' and cpu_arch_label = 'arm64' and machine_label = 'c7gd.4xlarge'
      then 'Ubuntu 24.04 (arm64, 16 vCPU)'
    when os = 'macos' and cpu_arch_label = 'arm64' and machine_label = 'mac-m4.metal'
      then 'macOS m4 (arm64, 10 vCPU)'
    when os = 'windows' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then 'Windows Server 2025 (amd64, 16 vCPU)'
    else
      (case coalesce(os, 'unknown')
        when 'linux' then 'Linux'
        when 'macos' then 'macOS'
        when 'windows' then 'Windows'
        else coalesce(os, 'Unknown OS')
      end)
      || ' ('
      || (case cpu_arch_label when 'x86_64' then 'amd64' else cpu_arch_label end)
      || ', '
      || (case machine_label when 'unspecified' then 'instance unspecified' else machine_label end)
      || ')'
  end as platform_label,
  case
    when os = 'linux' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then '32 GiB'
    when os = 'linux' and cpu_arch_label = 'arm64' and machine_label = 'c7gd.4xlarge'
      then '32 GiB'
    when os = 'macos' and cpu_arch_label = 'arm64' and machine_label = 'mac-m4.metal'
      then '24 GiB'
    when os = 'windows' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then '32 GiB'
    else 'Unknown'
  end as memory_label
from benchmarks.geomean_runs
where storage_type = 'duckdb'
  and merge_commit_date is not null
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
    defaultValue="linux|x86_64|c6id.4xlarge"
    title="Platform"
    description="OS, CPU architecture and machine type"
/>
<span class="mt-4 text-xs font-medium">Memory: {selected_platform?.[0]?.memory_label ?? 'Unknown'}</span>


<!-- dataLoaded: without it the warning flashes while the query is still running -->
{#if geomean.dataLoaded && geomean.length === 0}
<Alert status="warning">
No runs match this platform and date range.
</Alert>
{/if}

```sql geomean
select
  r.benchmark_series,
  r.benchmark,
  r.scale_factor_label,
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
  -- not displayed: version_baselines matches release runs to the plotted runs on these
  r.os,
  r.queries_sha
from benchmarks.geomean_runs r
join ${platform_options} p
  on p.platform_id = '${inputs.platform_select.value}'
 and p.machine_label = r.machine_label
 and p.cpu_arch_label = r.cpu_arch_label
 and p.os is not distinct from r.os
where r.storage_type = 'duckdb'
  -- the date filter is on the commit's merge date, not on when it was benchmarked. Release runs
  -- have no merge date and are deliberately excluded; they remain as the baselines below.
  --
  -- The end bound is two days past the input on purpose. The picker emits bare dates, converted
  -- via UTC, so east of UTC it reports each day as the day before - picking Sep 3 sends
  -- '2026-09-02'. One day compensates for that, the other makes the end day inclusive. The window
  -- can come out a day wider than the picker shows, but never narrower; narrower would drop the
  -- commits merged on the window's last day.
  and r.merge_commit_date >= '${inputs.date_select.start}'
  and r.merge_commit_date <  '${inputs.date_select.end}'::date + interval 2 day
order by r.merge_commit_date, r.run_timestamp
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
-- the alpha runs. The window only decides which platform and query sets are on the chart to match.
select
  r.benchmark_series,
  r.duckdb_version,
  -- the latest matching run of that version
  arg_max(r.geomean_seconds, r.run_timestamp) as baseline_seconds
from benchmarks.geomean_runs r
where r.storage_type = 'duckdb'
  and r.duckdb_version in ('v1.4.5', 'v1.5.5')
  -- the geomean query already carries the platform filter and identifies each benchmark series,
  -- so matching a row of it applies both here too
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

<script>
  const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  // Evidence normalizes timestamp strings without a timezone before passing them to ECharts.
  // Local getters preserve those UTC wall-clock values instead of applying the browser timezone.
  const chartDate = (value) => {
    if (value instanceof Date) return value;
    if (typeof value === 'number') return new Date(value);
    return new Date(String(value).replace(' ', 'T').replace(/Z$/, ''));
  };

  const chartTime = (value) => chartDate(value).getTime();
  const twoDigits = (value) => String(value).padStart(2, '0');

  const shortDate = (value) => {
    const date = chartDate(value);
    return Number.isNaN(date.getTime()) ? String(value) : `${MONTHS[date.getMonth()]} ${date.getDate()}`;
  };

  const fullTimestamp = (value) => {
    const date = chartDate(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return `${date.getFullYear()}-${twoDigits(date.getMonth() + 1)}-${twoDigits(date.getDate())} ${twoDigits(date.getHours())}:${twoDigits(date.getMinutes())}:${twoDigits(date.getSeconds())} UTC`;
  };

  const isCommitSha = (value) => /^[0-9a-f]{7,40}$/i.test(value ?? '');

  const benchmarkChartOptions = (rows) => {
    // A commit can have multiple benchmark runs. De-duplicate it before assigning predecessors so
    // repeated runs compare against the previous benchmarked commit, not against each other.
    const commits = [...new Set(
      rows
        .filter((row) => isCommitSha(row.commit_sha))
        .sort((a, b) => chartTime(a.merge_commit_date) - chartTime(b.merge_commit_date)
          || a.commit_sha.localeCompare(b.commit_sha))
        .map((row) => row.commit_sha)
    )];
    const previousCommitBySha = new Map(
      commits.slice(1).map((commitSha, index) => [commitSha, commits[index]])
    );

    return {
      xAxis: {
        // splitNumber is a target rather than a hard count. The one-day minimum prevents a 30- or
        // 90-day view from filling the axis with timestamp-level ticks.
        splitNumber: 6,
        minInterval: 24 * 60 * 60 * 1000,
        axisLabel: { formatter: shortDate }
      },
      tooltip: {
        trigger: 'item',
        renderMode: 'html',
        enterable: true,
        hideDelay: 300,
        confine: true,
        formatter: (params) => {
          const point = Array.isArray(params) ? params[0] : params;
          if (!Array.isArray(point?.value)) return '';

          const [timestamp, geomean] = point.value;
          const row = rows.find((candidate) =>
            chartTime(candidate.merge_commit_date) === chartTime(timestamp)
            && Number(candidate.geomean_seconds) === Number(geomean)
          );
          const commitSha = row?.commit_sha ?? '';
          const commitLabel = row?.commit ?? commitSha.slice(0, 8);
          const previousCommitSha = previousCommitBySha.get(commitSha);
          const commitLink = isCommitSha(commitSha)
            ? `<a href="https://github.com/duckdb/duckdb/commit/${commitSha}" target="_blank" rel="noopener noreferrer" style="text-decoration: underline;">${commitLabel}</a>`
            : 'Unknown';
          const comparisonLinks = previousCommitSha
            ? ` (<a href="https://github.com/duckdb/duckdb/compare/${previousCommitSha}..${commitSha}" target="_blank" rel="noopener noreferrer" style="text-decoration: underline;">range</a>, <a href="https://github.com/duckdb/duckdb/compare/${previousCommitSha}...${commitSha}" target="_blank" rel="noopener noreferrer" style="text-decoration: underline;">PRs</a>)`
            : '';
          const seconds = Number.isFinite(Number(geomean)) ? Number(geomean).toFixed(3) : 'Unknown';
          const mergedAt = fullTimestamp(row?.merge_commit_date ?? timestamp);

          return `<strong>Merged ${mergedAt}</strong><br>geomean (sec): ${seconds}<br>commit: ${commitLink}${comparisonLinks}`;
        }
      }
    };
  };
</script>

## TPC-DS @ sf100

<LineChart
    data={geomean.filter(d => d.benchmark_series === 'tpcds @ sf100')}
    x=merge_commit_date
    xType=time
    echartsOptions={benchmarkChartOptions(geomean.filter(d => d.benchmark_series === 'tpcds @ sf100'))}
    y=geomean_seconds
    yFmt=num3
    yMax={chart_bounds.find(b => b.benchmark_series === 'tpcds @ sf100')?.y_max}
    yAxisTitle="geomean (sec)"
    markers=true
    lineWidth=0
>
    <ReferenceLine
        data={version_baselines.filter(d => d.benchmark_series === 'tpcds @ sf100' && d.duckdb_version === 'v1.4.5')}
        y=baseline_seconds
        label=duckdb_version
        hideValue=true
        lineType=dashed
        color={['#c2410c', '#fb923c']}
        labelPosition=aboveEnd
        emptySet=pass
    />
    <ReferenceLine
        data={version_baselines.filter(d => d.benchmark_series === 'tpcds @ sf100' && d.duckdb_version === 'v1.5.5')}
        y=baseline_seconds
        label=duckdb_version
        hideValue=true
        lineType=dashed
        color={['#0f766e', '#2dd4bf']}
        labelPosition=belowEnd
        emptySet=pass
    />
</LineChart>

## TPC-H @ sf100

<LineChart
    data={geomean.filter(d => d.benchmark_series === 'tpch @ sf100')}
    x=merge_commit_date
    xType=time
    echartsOptions={benchmarkChartOptions(geomean.filter(d => d.benchmark_series === 'tpch @ sf100'))}
    y=geomean_seconds
    yFmt=num3
    yMax={chart_bounds.find(b => b.benchmark_series === 'tpch @ sf100')?.y_max}
    yAxisTitle="geomean (sec)"
    markers=true
    lineWidth=0
>
    <ReferenceLine
        data={version_baselines.filter(d => d.benchmark_series === 'tpch @ sf100' && d.duckdb_version === 'v1.4.5')}
        y=baseline_seconds
        label=duckdb_version
        hideValue=true
        lineType=dashed
        color={['#c2410c', '#fb923c']}
        labelPosition=aboveEnd
        emptySet=pass
    />
    <ReferenceLine
        data={version_baselines.filter(d => d.benchmark_series === 'tpch @ sf100' && d.duckdb_version === 'v1.5.5')}
        y=baseline_seconds
        label=duckdb_version
        hideValue=true
        lineType=dashed
        color={['#0f766e', '#2dd4bf']}
        labelPosition=belowEnd
        emptySet=pass
    />
</LineChart>

## ClickBench

<LineChart
    data={geomean.filter(d => d.benchmark_series === 'clickbench')}
    x=merge_commit_date
    xType=time
    echartsOptions={benchmarkChartOptions(geomean.filter(d => d.benchmark_series === 'clickbench'))}
    y=geomean_seconds
    yFmt=num3
    yMax={chart_bounds.find(b => b.benchmark_series === 'clickbench')?.y_max}
    yAxisTitle="geomean (sec)"
    markers=true
    lineWidth=0
>
    <ReferenceLine
        data={version_baselines.filter(d => d.benchmark_series === 'clickbench' && d.duckdb_version === 'v1.4.5')}
        y=baseline_seconds
        label=duckdb_version
        hideValue=true
        lineType=dashed
        color={['#c2410c', '#fb923c']}
        labelPosition=aboveEnd
        emptySet=pass
    />
    <ReferenceLine
        data={version_baselines.filter(d => d.benchmark_series === 'clickbench' && d.duckdb_version === 'v1.5.5')}
        y=baseline_seconds
        label=duckdb_version
        hideValue=true
        lineType=dashed
        color={['#0f766e', '#2dd4bf']}
        labelPosition=belowEnd
        emptySet=pass
    />
</LineChart>

## Runs

A run with failed queries is still plotted, but its geomean covers fewer queries than a complete
run - `# failed` is what tells them apart.

```sql run_table
select
  benchmark_series,
  merge_date,
  duckdb_version,
  commit,
  round(geomean_seconds, 3) as 'geomean (sec)',
  queries_ok as '# ok',
  queries_failed as '# failed',
  platform,
  query_set
from ${geomean}
order by merge_commit_date desc, run_timestamp desc
```

<DataTable data={run_table} rows=25 search=true>
    <Column id=benchmark_series />
    <Column id=merge_date />
    <Column id=duckdb_version />
    <Column id=commit />
    <Column id='geomean (sec)' fmt=num3 />
    <Column id='# ok' />
    <Column id='# failed' />
    <Column id=platform />
    <Column id=query_set />
</DataTable>

## Per-query execution times

The individual queries of a single run, each against the two release baselines on the selected
platform. Every timing of the release baselines is a median over that query's warm runs.

`ratio vs ...` is the selected run divided by the baseline: **above 1.0 means the selected run is
slower** than that release, below 1.0 means faster. A ratio above 1.1 is shaded red and one below
0.9 green; the band in between is left uncoloured. Rows are sorted by the
v1.5.5 ratio, so regressions are at the top and improvements at the bottom. A blank baseline means
that release has no run of this query - v1.4.5 has no DuckLake runs at all. Failed queries are
listed with empty timings.

```sql run_options
select
  r.run_id,
  r.run_date || '  -  ' || r.benchmark_series || '  -  ' || r.duckdb_version as run_label,
  r.run_timestamp
from benchmarks.geomean_runs r
join ${platform_options} p
  on p.platform_id = '${inputs.platform_select.value}'
 and p.machine_label = r.machine_label
 and p.cpu_arch_label = r.cpu_arch_label
 and p.os is not distinct from r.os
where r.storage_type = 'duckdb'
  -- same date filter as the geomean query above - see there for the two-day end bound
  and r.merge_commit_date >= '${inputs.date_select.start}'
  and r.merge_commit_date <  '${inputs.date_select.end}'::date + interval 2 day
order by r.run_timestamp desc
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
  from benchmarks.query_times q
  join ${platform_options} p
    on p.platform_id = '${inputs.platform_select.value}'
   and p.machine_label = q.machine_label
   and p.cpu_arch_label = q.cpu_arch_label
   and p.os is not distinct from q.os
  where q.storage_type = 'duckdb'
    and q.duckdb_version in ('v1.4.5', 'v1.5.5')
  group by q.benchmark_series, q.query, q.duckdb_version
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
