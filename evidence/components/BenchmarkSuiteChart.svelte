<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { LineChart, ReferenceLine } from '@evidence-dev/core-components';

  export let data = [];
  export let baselines = [];
  export let bounds = [];
  export let series;

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

  const chartOptions = (rows) => {
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

  $: rows = data?.filter?.((row) => row.benchmark_series === series) ?? [];
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
{/if}
