<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { LineChart, ReferenceLine } from '@evidence-dev/core-components';
  import BenchmarkQueryLink from './BenchmarkQueryLink.svelte';
  import {
    chartTime,
    commitLinks,
    escapeHtml,
    formatSeconds,
    fullTimestamp,
    previousCommitMap,
    shortDate,
    versionLine,
    versionLineColors,
    versionLineOrder
  } from './benchmarkChartUtils.js';

  export let data = [];
  export let baselines = [];
  export let bounds = [];
  export let series;
  export let storage = undefined;
  export let platform = undefined;
  export let dateStart = undefined;
  export let dateEnd = undefined;
  export let version = undefined;

  const chartOptions = (rows) => {
    const previousCommitBySha = previousCommitMap(rows);

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
          const previousCommitSha = previousCommitBySha.get(commitSha);
          const seconds = formatSeconds(geomean);
          const mergedAt = fullTimestamp(row?.merge_commit_date ?? timestamp);
          const version = escapeHtml(row?.duckdb_version ?? point.seriesName ?? 'Unknown');

          return `<strong>Merged ${mergedAt}</strong><br>version: ${version}<br>geomean (sec): ${seconds}<br>commit: ${commitLinks(row, previousCommitSha)}`;
        }
      }
    };
  };

  // One plotted series per release line, so main and the maintenance branch are separate colours.
  $: rows = (data?.filter?.((row) => row.benchmark_series === series) ?? [])
    .map((row) => ({ ...row, version_line: versionLine(row.duckdb_version) }));
  $: lines = rows.map((row) => row.version_line);
  $: lineOrder = versionLineOrder(lines);
  $: lineColors = versionLineColors(lineOrder);
  $: baselineRows = baselines?.filter?.((row) => row.benchmark_series === series) ?? [];
  $: yMax = bounds?.find?.((row) => row.benchmark_series === series)?.y_max;
</script>

{#if rows.length > 0}
  <LineChart
      data={rows}
      x=merge_commit_date
      xType=time
      echartsOptions={chartOptions(rows)}
      y=geomean_seconds
      series=version_line
      seriesOrder={lineOrder}
      seriesColors={lineColors}
      yFmt=num3
      {yMax}
      yAxisTitle="geomean (sec)"
      markers=true
      lineWidth=0
  >
      <ReferenceLine
          data={baselineRows.filter((row) => row.duckdb_version === 'v1.4.5')}
          y=baseline_seconds
          label=duckdb_version
          hideValue=true
          lineType=dashed
          color={['#c2410c', '#fb923c']}
          labelPosition=aboveEnd
          emptySet=pass
      />
      <ReferenceLine
          data={baselineRows.filter((row) => row.duckdb_version === 'v1.5.5')}
          y=baseline_seconds
          label=duckdb_version
          hideValue=true
          lineType=dashed
          color={['#0f766e', '#2dd4bf']}
          labelPosition=belowEnd
          emptySet=pass
      />
  </LineChart>
  {#if storage && platform && dateStart && dateEnd}
    <BenchmarkQueryLink
        {storage}
        {series}
        {platform}
        start={dateStart}
        end={dateEnd}
        {version}
    />
  {/if}
{/if}
