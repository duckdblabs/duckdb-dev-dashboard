import test from 'node:test';
import assert from 'node:assert/strict';
import { hasQueryTiming, queryFailureSummary } from './benchmarkFailureUtils.js';

const row = (day, status = 'ok', extra = {}) => ({
  run_id: `run-${day}`,
  merge_commit_date: `2026-09-${day}T10:00:00Z`,
  run_timestamp: `2026-09-${day}T12:00:00Z`,
  status,
  mean_seconds: status === 'ok' ? 1 : null,
  ...extra
});

test('old failures remain inspectable without an active warning after recovery', () => {
  const failed = [18, 19, 20, 21].map((day) => row(day, 'failed'));
  const result = queryFailureSummary([row(25), ...failed]);

  assert.equal(result.latestFailed, false);
  assert.equal(result.failures.length, 4);
  assert.equal(result.lastFailure.run_id, 'run-21');
  assert.deepEqual(result.failures.map((run) => run.run_id), ['run-21', 'run-20', 'run-19', 'run-18']);
});

test('a failure in the latest selected commit raises an active warning', () => {
  const result = queryFailureSummary([row(25, 'failed'), row(18), row(24)]);

  assert.equal(result.latestFailed, true);
  assert.equal(result.latest.run_id, 'run-25');
});

test('a recent rerun of an older commit does not override the latest commit', () => {
  const result = queryFailureSummary([
    row(18, 'failed', { run_timestamp: '2026-10-20T12:00:00Z' }),
    row(25)
  ]);

  assert.equal(result.latestFailed, false);
  assert.equal(result.latest.run_id, 'run-25');
});

test('the latest rerun of the same commit determines whether it recovered', () => {
  const result = queryFailureSummary([
    row(25, 'ok', { run_id: 'retry', run_timestamp: '2026-09-25T13:00:00Z' }),
    row(25, 'failed')
  ]);

  assert.equal(result.latestFailed, false);
  assert.equal(result.latest.run_id, 'retry');
  assert.equal(result.failures.length, 1);
});

test('missing or invalid successful timings do not become failures or zero-valued points', () => {
  for (const value of [null, undefined, '', 0, -1, Infinity, NaN]) {
    const successful = row(25, 'ok', { mean_seconds: value });
    assert.equal(hasQueryTiming(successful), false);
    assert.equal(queryFailureSummary([successful]).failures.length, 0);
  }
  assert.equal(hasQueryTiming(row(25, 'ok', { mean_seconds: '0.5' })), true);
  assert.equal(hasQueryTiming(row(25, 'failed', { mean_seconds: 1 })), false);
  assert.equal(queryFailureSummary([]).latestFailed, false);
});
