<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { ECharts } from '@evidence-dev/core-components';
  import {
    baselineColor,
    chartDate,
    chartTime,
    commitLinks,
    dateAxisInterval,
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
  import { hasQueryTiming, isQueryFailure, queryFailureSummary } from './benchmarkFailureUtils.js';

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
    const successful = rows.filter(hasQueryTiming);
    const failed = rows.filter(isQueryFailure);
    const values = [
      ...successful.map((row) => Number(row.mean_seconds)),
      ...baselineRows.map((row) => Number(row.baseline_seconds)).filter(Number.isFinite)
    ];
    const yUnit = timingUnit(values);
    const yMax = niceAxisMax((values.length ? Math.max(...values) : 1) * 1.08);
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
          color: baselineColor(row.duckdb_version)
        },
        endLabel: {
          show: true,
          formatter: row.duckdb_version,
          color: baselineColor(row.duckdb_version),
          fontSize: 10
        }
      }));

    return {
      animation: false,
      grid: { left: 58, right: 52, top: 12, bottom: 40 },
      tooltip: {
        trigger: 'item',
        renderMode: 'html',
        enterable: true,
        hideDelay: 300,
        confine: true,
        extraCssText: 'max-width: min(420px, 80vw); white-space: normal; overflow-wrap: anywhere;',
        formatter: tooltip
      },
      xAxis: {
        type: 'value',
        min: xMin,
        max: xMax,
        splitNumber: 10,
        interval: dateAxisInterval(xMin, xMax),
        minInterval: 24 * 60 * 60 * 1000,
        splitLine: { show: false },
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
        name: `mean (${yUnit.label})`,
        nameLocation: 'middle',
        nameGap: 43,
        axisLabel: { formatter: (value) => formatTimingTick(value, yUnit) }
      },
      series: [
        ...meanSeries,
        {
          name: 'failed',
          type: 'scatter',
          data: failed.map((row) => ({ value: [chartTime(row.merge_commit_date), 0], row })),
          // A visible symbol is also the tooltip's hover target. The old 1px invisible point
          // left only a text label to see, which was very difficult to hover.
          symbol: 'path://M-5,-3L-3,-5L0,-2L3,-5L5,-3L2,0L5,3L3,5L0,2L-3,5L-5,3L-2,0Z',
          symbolSize: 12,
          itemStyle: { color: '#ef4444' },
          clip: false,
          z: 4
        },
        ...baselineSeries
      ]
    };
  };

  $: rows = data?.filter?.((row) => row.query === query) ?? [];
  $: baselineRows = baselines?.filter?.((row) => row.query === query) ?? [];
  $: config = chartConfig(rows, baselineRows);
  $: failureSummary = queryFailureSummary(rows);
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
      {#if failureSummary.latestFailed}
        <span
            class="text-xs font-medium text-negative"
            title={`The latest run in the selected window failed: ${failureSummary.latest.error ?? 'No error was recorded.'}`}
        >Latest run failed</span>
      {/if}
    </div>
  </div>
  <ECharts {config} data={rows} height="250px" />
</section>
