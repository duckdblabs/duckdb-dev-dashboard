export const RECENT_POINT_COUNT = 3;
export const BASELINE_POINT_COUNT = 5;
export const MIN_SLOWDOWN_RATIO = 1.05;
export const MIN_BASELINE_SECONDS = 0.05;
export const RELEASE_POINT_COUNT = 5;
export const MIN_RELEASE_SLOWDOWN_RATIO = 1.10;

const timestamp = (value) => {
  if (typeof value === 'number') return value;
  const parsed = value instanceof Date ? value.getTime() : Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const compareRows = (left, right) =>
  timestamp(left.merge_commit_date) - timestamp(right.merge_commit_date)
  || timestamp(left.run_timestamp) - timestamp(right.run_timestamp)
  || String(left.run_id ?? '').localeCompare(String(right.run_id ?? ''));

const median = (values) => {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
};

const successfulRowsForLatestQuerySet = (rows) => {
  const ordered = [...rows].sort(compareRows);
  const latestRow = ordered.at(-1);
  if (!latestRow) return [];

  // Changing the query set starts a new comparison window. In particular, do not compare a
  // freshly edited query with timings from its previous definition.
  const latestQuerySet = latestRow.queries_sha ?? null;
  return ordered.filter((row) =>
    (row.queries_sha ?? null) === latestQuerySet
    && row.status === 'ok'
    && Number.isFinite(Number(row.mean_seconds))
    && Number(row.mean_seconds) > 0
  );
};

export const scoreRecentlySlower = (rows = []) => {
  const successful = successfulRowsForLatestQuerySet(rows);
  const requiredPoints = RECENT_POINT_COUNT + BASELINE_POINT_COUNT;
  if (successful.length < requiredPoints) return null;

  const comparison = successful.slice(-requiredPoints);
  const baselineValues = comparison
    .slice(0, BASELINE_POINT_COUNT)
    .map((row) => Number(row.mean_seconds));
  const recentValues = comparison
    .slice(BASELINE_POINT_COUNT)
    .map((row) => Number(row.mean_seconds));
  const baselineMedian = median(baselineValues);
  const recentMedian = median(recentValues);
  const ratio = recentMedian / baselineMedian;

  return {
    kind: 'recently_slower',
    isSlower: baselineMedian > MIN_BASELINE_SECONDS && ratio >= MIN_SLOWDOWN_RATIO,
    recentMedian,
    baselineMedian,
    ratio,
    percentChange: (ratio - 1) * 100
  };
};

export const scoreReleaseSlower = (rows = [], baselineSeconds, releaseVersion) => {
  const baselineMedian = Number(baselineSeconds);
  if (!Number.isFinite(baselineMedian) || baselineMedian <= 0) return null;

  const successful = successfulRowsForLatestQuerySet(rows);
  if (successful.length < RELEASE_POINT_COUNT) return null;

  const recentMedian = median(successful
    .slice(-RELEASE_POINT_COUNT)
    .map((row) => Number(row.mean_seconds)));
  const ratio = recentMedian / baselineMedian;

  return {
    kind: 'release_slower',
    isSlower: baselineMedian > MIN_BASELINE_SECONDS && ratio >= MIN_RELEASE_SLOWDOWN_RATIO,
    releaseVersion,
    recentMedian,
    baselineMedian,
    ratio,
    percentChange: (ratio - 1) * 100
  };
};

const versionParts = (version) => {
  const match = /^v?(\d+)\.(\d+)\.(\d+)/.exec(String(version ?? ''));
  return match ? match.slice(1).map(Number) : null;
};

export const latestAnnotationVersion = (baselines = []) => [...new Set(
  baselines.map((row) => row.duckdb_version).filter((version) => versionParts(version))
)].sort((left, right) => {
  const leftParts = versionParts(left);
  const rightParts = versionParts(right);
  return leftParts[0] - rightParts[0]
    || leftParts[1] - rightParts[1]
    || leftParts[2] - rightParts[2];
}).at(-1) ?? null;
