import { chartTime } from './benchmarkChartUtils.js';

export const isQueryFailure = (row) => row.status !== 'ok';

export const hasQueryTiming = (row) => !isQueryFailure(row)
  && row.mean_seconds != null
  && Number.isFinite(Number(row.mean_seconds))
  && Number(row.mean_seconds) > 0;

const compareRuns = (left, right) =>
  chartTime(left.merge_commit_date) - chartTime(right.merge_commit_date)
  || chartTime(left.run_timestamp) - chartTime(right.run_timestamp)
  || String(left.run_id ?? '').localeCompare(String(right.run_id ?? ''));

export const queryFailureSummary = (rows = []) => {
  // Use the newest commit in the selected window, then its latest rerun. A recent benchmark of
  // an older commit must not turn a recovered query into an active failure warning.
  const ordered = [...rows].sort(compareRuns);
  const latest = ordered.at(-1);
  const failures = ordered.filter(isQueryFailure).reverse();
  return {
    failures,
    latest,
    latestFailed: Boolean(latest && isQueryFailure(latest)),
    lastFailure: failures[0]
  };
};
