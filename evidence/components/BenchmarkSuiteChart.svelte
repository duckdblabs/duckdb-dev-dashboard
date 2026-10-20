<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { ECharts } from '@evidence-dev/core-components';
  import BenchmarkQueryLink from './BenchmarkQueryLink.svelte';
  import {
    baselineColor,
    chartDate,
    chartTime,
    commitLinks,
    escapeHtml,
    formatSeconds,
    formatTimingTick,
    fullTimestamp,
    niceAxisMax,
    previousCommitMap,
    shortDate,
    timingUnit,
    versionLine,
    versionLineColors,
    versionLineOrder
  } from './benchmarkChartUtils.js';

  export let data = [];
  export let baselines = [];
  export let failures = [];
  export let series;
  export let storage = undefined;
  export let platform = undefined;
  export let dateStart = undefined;
  export let dateEnd = undefined;
  export let version = undefined;

  const dateBound = (value, extraDays = 0) => {
    const date = chartDate(value);
    if (Number.isNaN(date.getTime())) return undefined;
    date.setDate(date.getDate() + extraDays);
    return date.getTime();
  };

  const isComplete = (row) => Number(row.queries_failed ?? 0) === 0;

  const chartConfig = (rows, baselineRows, failedByRun) => {
    const previousCommitBySha = previousCommitMap(rows);
    const lineOrder = versionLineOrder(rows.map((row) => versionLine(row.duckdb_version)));
    const lineColors = versionLineColors(lineOrder);

    const values = [
      ...rows.map((row) => Number(row.geomean_seconds)),
      ...baselineRows.map((row) => Number(row.baseline_seconds))
    ].filter(Number.isFinite);
    const yUnit = timingUnit(values);
    // headroom above the tallest element: a reference line exactly at the chart maximum sits on
    // the plot border and is indistinguishable from it. Round the result so the top tick remains
    // as readable as the regular interval ticks below it.
    const yMax = niceAxisMax((values.length ? Math.max(...values) : 1) * 1.08);
    const times = rows.map((row) => chartTime(row.merge_commit_date));
    const xMin = dateBound(dateStart) ?? Math.min(...times);
    // the page query's end bound is end + 2 days; see the geomean query for why
    const xMax = dateBound(dateEnd, 2) ?? Math.max(...times);

    const tooltip = (params) => {
      const row = params?.data?.row;
      if (!row) return '';
      const previousCommitSha = previousCommitBySha.get(row.commit_sha);
      const mergedAt = escapeHtml(fullTimestamp(row.merge_commit_date));
      const lines = [
        `<strong>Merged ${mergedAt}</strong>`,
        `version: ${escapeHtml(row.duckdb_version ?? 'Unknown')}`,
        `geomean (sec): ${formatSeconds(row.geomean_seconds)}`,
        `commit: ${commitLinks(row, previousCommitSha)}`
      ];

      if (!isComplete(row)) {
        const failed = failedByRun.get(row.run_id);
        lines.push(
          `<span style="color:#ef4444;font-weight:600;">${row.queries_ok} of ${row.queries_attempted} queries ok</span>`
          + (failed ? ` — failed: ${escapeHtml(failed)}` : '')
        );
      }
      return lines.join('<br>');
    };

    const runSeries = lineOrder.map((line) => ({
      name: line,
      type: 'line',
      data: rows
        .filter((row) => versionLine(row.duckdb_version) === line)
        .map((row) => ({
          value: [chartTime(row.merge_commit_date), Number(row.geomean_seconds)],
          row,
          // hollow marker: the run lost queries, so its geomean is over a smaller set
          ...(isComplete(row) ? {} : {
            itemStyle: { color: '#ffffff', borderColor: lineColors[line], borderWidth: 2 }
          })
        })),
      showSymbol: true,
      symbol: 'circle',
      symbolSize: 8,
      lineStyle: { width: 0 },
      itemStyle: { color: lineColors[line] }
    }));

    const baselineSeries = baselineRows
      .filter((row) => Number.isFinite(Number(row.baseline_seconds)))
      .map((row) => ({
        name: row.duckdb_version,
        type: 'line',
        data: [[xMin, Number(row.baseline_seconds)], [xMax, Number(row.baseline_seconds)]],
        showSymbol: false,
        silent: true,
        lineStyle: { type: 'dashed', width: 1, color: baselineColor(row.duckdb_version) },
        endLabel: {
          show: true,
          formatter: row.duckdb_version,
          color: baselineColor(row.duckdb_version),
          fontSize: 10
        }
      }));

    return {
      animation: false,
      grid: { left: 64, right: 56, top: 16, bottom: 44 },
      legend: lineOrder.length > 1
        ? { show: true, top: 0, right: 56, data: lineOrder }
        : { show: false },
      tooltip: {
        trigger: 'item',
        renderMode: 'html',
        enterable: true,
        hideDelay: 300,
        confine: true,
        formatter: tooltip
      },
      xAxis: {
        type: 'time',
        min: xMin,
        max: xMax,
        splitNumber: 2,
        minInterval: 24 * 60 * 60 * 1000,
        axisLabel: {
          formatter: shortDate,
          rotate: 45,
          margin: 8,
          hideOverlap: true,
          showMinLabel: false,
          showMaxLabel: false,
          align: 'right',
          verticalAlign: 'middle'
        }
      },
      yAxis: {
        type: 'value',
        min: 0,
        max: yMax,
        name: `geomean (${yUnit.label})`,
        nameLocation: 'middle',
        nameGap: 48,
        axisLabel: { formatter: (value) => formatTimingTick(value, yUnit) }
      },
      series: [...runSeries, ...baselineSeries]
    };
  };

  $: rows = data?.filter?.((row) => row.benchmark_series === series) ?? [];
  $: baselineRows = baselines?.filter?.((row) => row.benchmark_series === series) ?? [];
  $: runIds = new Set(rows.map((row) => row.run_id));
  $: failedByRun = new Map(
    (failures ?? []).filter((row) => runIds.has(row.run_id)).map((row) => [row.run_id, row.failed_queries])
  );
  $: incompleteCount = rows.filter((row) => !isComplete(row)).length;
  $: config = chartConfig(rows, baselineRows, failedByRun);
</script>

{#if rows.length > 0}
  {#if incompleteCount > 0}
    <p class="text-xs text-base-content-muted">
      {incompleteCount} of {rows.length} runs are incomplete (hollow markers): some queries failed, so their
      geomean is over fewer queries and not directly comparable.
    </p>
  {/if}
  <ECharts {config} data={rows} height="320px" />
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
