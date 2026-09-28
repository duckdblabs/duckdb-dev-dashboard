import test from 'node:test';
import assert from 'node:assert/strict';

import {
  MIN_BASELINE_SECONDS,
  latestAnnotationVersion,
  scoreRecentlySlower,
  scoreReleaseSlower
} from './benchmarkRegressionUtils.js';

const rows = (seconds, options = {}) => seconds.map((mean_seconds, index) => ({
  run_id: `run-${index}`,
  run_timestamp: `2026-09-${String(index + 1).padStart(2, '0')}T02:00:00Z`,
  merge_commit_date: `2026-09-${String(index + 1).padStart(2, '0')}T01:00:00Z`,
  queries_sha: options.queriesSha ?? 'query-set-a',
  status: 'ok',
  mean_seconds
}));

test('one anomalous recent point does not count as a regression', () => {
  const result = scoreRecentlySlower(rows([1, 1, 1, 1, 1, 1, 1, 2]));

  assert.equal(result.isSlower, false);
  assert.equal(result.recentMedian, 1);
});

test('two sustained slower points count as a regression', () => {
  const result = scoreRecentlySlower(rows([1, 1, 1, 1, 1, 1.06, 1.07, 1]));

  assert.equal(result.isSlower, true);
  assert.ok(Math.abs(result.percentChange - 6) < 1e-9);
});

test('the exact five percent boundary is included', () => {
  const result = scoreRecentlySlower(rows([1, 1, 1, 1, 1, 1.05, 1.05, 1.05]));

  assert.equal(result.isSlower, true);
});

test('a baseline at or below 50 ms is excluded as noisy', () => {
  const below = scoreRecentlySlower(rows([0.049, 0.049, 0.049, 0.049, 0.049, 0.06, 0.06, 0.06]));
  const boundary = scoreRecentlySlower(rows([
    MIN_BASELINE_SECONDS,
    MIN_BASELINE_SECONDS,
    MIN_BASELINE_SECONDS,
    MIN_BASELINE_SECONDS,
    MIN_BASELINE_SECONDS,
    0.06,
    0.06,
    0.06
  ]));
  const above = scoreRecentlySlower(rows([0.051, 0.051, 0.051, 0.051, 0.051, 0.054, 0.054, 0.054]));

  assert.equal(below.isSlower, false);
  assert.equal(boundary.isSlower, false);
  assert.equal(above.isSlower, true);
});

test('failures and invalid timings are ignored', () => {
  const data = rows([1, 1, 1, 1, 1, 1.06, 1.06, 1.06]);
  data.splice(6, 0, {
    ...data[5],
    run_id: 'failed',
    merge_commit_date: '2026-09-06T12:00:00Z',
    status: 'failed',
    mean_seconds: null
  });
  data.splice(7, 0, {
    ...data[5],
    run_id: 'invalid',
    merge_commit_date: '2026-09-06T13:00:00Z',
    mean_seconds: Number.NaN
  });

  assert.equal(scoreRecentlySlower(data).isSlower, true);
});

test('insufficient history is not scored', () => {
  assert.equal(scoreRecentlySlower(rows([1, 1, 1, 1, 1, 1.1, 1.1])), null);
});

test('the latest query set is never compared with an older query set', () => {
  const oldRows = rows([1, 1, 1, 1, 1, 1, 1, 1]);
  const newRows = rows([2, 2, 2], { queriesSha: 'query-set-b' }).map((row, index) => ({
    ...row,
    run_id: `new-${index}`,
    merge_commit_date: `2026-10-${String(index + 1).padStart(2, '0')}T01:00:00Z`
  }));

  assert.equal(scoreRecentlySlower([...oldRows, ...newRows]), null);
});

test('points are ordered by their benchmark timestamps rather than input order', () => {
  const data = rows([1, 1, 1, 1, 1, 1.06, 1.06, 1.06]);
  const shuffled = [data[7], data[2], data[5], data[0], data[6], data[4], data[1], data[3]];

  assert.equal(scoreRecentlySlower(shuffled).isSlower, true);
});

test('release comparison uses the median of the latest five successful points', () => {
  const result = scoreReleaseSlower(rows([0.5, 1.12, 1.11, 1.15, 1.13, 10]), 1, 'v1.5.5');

  assert.equal(result.isSlower, true);
  assert.equal(result.recentMedian, 1.13);
  assert.ok(Math.abs(result.percentChange - 13) < 1e-9);
});

test('the exact ten percent release boundary is included', () => {
  const result = scoreReleaseSlower(rows([1.1, 1.1, 1.1, 1.1, 1.1]), 1, 'v1.5.5');

  assert.equal(result.isSlower, true);
});

test('release comparison requires five points and a baseline above 50 ms', () => {
  assert.equal(scoreReleaseSlower(rows([1.2, 1.2, 1.2, 1.2]), 1, 'v1.5.5'), null);
  assert.equal(scoreReleaseSlower(rows([0.06, 0.06, 0.06, 0.06, 0.06]), 0.05, 'v1.5.5').isSlower, false);
  assert.equal(scoreReleaseSlower(rows([1.2, 1.2, 1.2, 1.2, 1.2]), null, 'v1.5.5'), null);
});

test('latest annotation is selected by semantic version', () => {
  const version = latestAnnotationVersion([
    { duckdb_version: 'v1.9.5' },
    { duckdb_version: 'v1.10.1' },
    { duckdb_version: 'v1.5.5' },
    { duckdb_version: 'development' }
  ]);

  assert.equal(version, 'v1.10.1');
  assert.equal(latestAnnotationVersion([]), null);
});
