<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { ECharts } from '@evidence-dev/core-components';
  import {
    chartDate,
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
  export let query;
  export let dateStart;
  export let dateEnd;
  export let comparison = null;

  const dateBound = (value, extraDays = 0) => {
    const date = chartDate(value);
    if (Number.isNaN(date.getTime())) return undefined;
    date.setDate(date.getDate() + extraDays);
    return date.getTime();
  };

  const chartConfig = (rows, baselineRows) => {
    const previousCommitBySha = previousCommitMap(rows);
    const successful = rows.filter((row) => row.status === 'ok' && Number.isFinite(Number(row.mean_seconds)));
    const failed = rows.filter((row) => row.status !== 'ok' || !Number.isFinite(Number(row.mean_seconds)));
    const values = [
      ...successful.map((row) => Number(row.mean_seconds)),
      ...baselineRows.map((row) => Number(row.baseline_seconds)).filter(Number.isFinite)
    ];
    const yMax = (values.length ? Math.max(...values) : 1) * 1.08;
    const xMin = dateBound(dateStart) ?? Math.min(...rows.map((row) => chartTime(row.merge_commit_date)));
    // Match the dashboard's inclusive end-date workaround: the page query uses end + 2 days.
    const xMax = dateBound(dateEnd, 2) ?? Math.max(...rows.map((row) => chartTime(row.merge_commit_date)));

    const tooltip = (params) => {
      const row = params?.data?.row;
      if (!row) return '';
      const previousCommitSha = previousCommitBySha.get(row.commit_sha);
      const mergedAt = escapeHtml(fullTimestamp(row.merge_commit_date));
      const querySet = escapeHtml(row.query_set ?? 'Unknown');
      const version = escapeHtml(row.duckdb_version ?? 'Unknown');

      if (params.seriesName === 'failed') {
        const error = row.error ? `<br>error: ${escapeHtml(row.error)}` : '';
        return `<strong>Merged ${mergedAt}</strong><br><span style="color:#ef4444;font-weight:600;">failed — excluded from geomean</span><br>version: ${version}<br>commit: ${commitLinks(row, previousCommitSha)}<br>query set: ${querySet}${error}`;
      }

      return `<strong>Merged ${mergedAt}</strong><br>version: ${version}<br>mean (sec): ${formatSeconds(row.mean_seconds)}<br>median (sec): ${formatSeconds(row.median_seconds)}<br>range (sec): ${formatSeconds(row.fastest_seconds)}–${formatSeconds(row.slowest_seconds)}<br>commit: ${commitLinks(row, previousCommitSha)}<br>query set: ${querySet}`;
    };

    // One series per release line, so main and the maintenance branch are separate colours.
    const lineOrder = versionLineOrder(successful.map((row) => versionLine(row.duckdb_version)));
    const lineColors = versionLineColors(lineOrder);
    const meanSeries = lineOrder.map((line) => ({
      name: line,
      type: 'line',
      data: successful
        .filter((row) => versionLine(row.duckdb_version) === line)
        .map((row) => ({
          value: [chartTime(row.merge_commit_date), Number(row.mean_seconds)],
          row
        })),
      showSymbol: true,
      symbol: 'circle',
      symbolSize: 7,
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
        lineStyle: {
          type: 'dashed',
          width: 1,
          color: row.duckdb_version === 'v1.4.5' ? '#ea580c' : '#0d9488'
        },
        endLabel: {
          show: true,
          formatter: row.duckdb_version,
          color: row.duckdb_version === 'v1.4.5' ? '#ea580c' : '#0d9488',
          fontSize: 10
        }
      }));

    return {
      animation: false,
      grid: { left: 58, right: 52, top: 12, bottom: 42 },
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
        splitNumber: 4,
        minInterval: 24 * 60 * 60 * 1000,
        axisLabel: { formatter: shortDate }
      },
      yAxis: {
        type: 'value',
        min: 0,
        max: yMax,
        name: 'mean (sec)',
        nameLocation: 'middle',
        nameGap: 43,
        axisLabel: { formatter: (value) => Number(value).toFixed(3) }
      },
      series: [
        ...meanSeries,
        {
          name: 'failed',
          type: 'scatter',
          data: failed.map((row) => ({ value: [chartTime(row.merge_commit_date), 0], row })),
          symbolSize: 1,
          itemStyle: { opacity: 0 },
          label: {
            show: true,
            formatter: '×',
            position: 'top',
            color: '#ef4444',
            fontSize: 18,
            fontWeight: 'bold'
          }
        },
        ...baselineSeries
      ]
    };
  };

  $: rows = data?.filter?.((row) => row.query === query) ?? [];
  $: baselineRows = baselines?.filter?.((row) => row.query === query) ?? [];
  $: config = chartConfig(rows, baselineRows);
  $: failedCount = rows.filter((row) => row.status !== 'ok' || !Number.isFinite(Number(row.mean_seconds))).length;
  $: comparisonTitle = comparison?.kind === 'release_slower'
    ? `Latest 5-point median: ${formatSeconds(comparison.recentMedian)} sec; ${comparison.releaseVersion} annotation: ${formatSeconds(comparison.baselineMedian)} sec`
    : `Latest 3-point median: ${formatSeconds(comparison?.recentMedian)} sec; preceding 5-point median: ${formatSeconds(comparison?.baselineMedian)} sec`;
</script>

<section id={`query-${query}`} class="min-h-[300px] rounded-md border border-base-300 bg-base-100 p-3">
  <div class="flex items-baseline justify-between gap-2">
    <h3 class="m-0 text-sm font-semibold">{query}</h3>
    <div class="flex flex-wrap items-center justify-end gap-2">
      {#if comparison?.isSlower}
        <span
            class="rounded border border-negative/50 bg-negative/10 px-1.5 py-0.5 text-xs font-medium text-negative"
            title={comparisonTitle}
        >
          +{comparison.percentChange.toFixed(1)}% slower
        </span>
      {/if}
      {#if failedCount > 0}
        <span class="text-xs font-medium text-negative">× {failedCount} failed</span>
      {/if}
    </div>
  </div>
  <ECharts {config} data={rows} height="250px" />
</section>
