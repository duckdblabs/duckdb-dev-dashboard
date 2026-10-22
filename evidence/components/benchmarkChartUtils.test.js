import test from 'node:test';
import assert from 'node:assert/strict';

import {
  baselineColor,
  chartDateBounds,
  dateAxisInterval,
  formatTimingTick,
  latestReleaseVersions,
  niceAxisMax,
  timingUnit
} from './benchmarkChartUtils.js';

const DAY_MS = 24 * 60 * 60 * 1000;

test('release-only charts span the selected dates without development points', () => {
  const bounds = chartDateBounds([], '2026-07-24', '2026-10-22');
  assert.equal(bounds.min, new Date('2026-07-24').getTime());
  assert.equal(bounds.max, new Date('2026-10-24').getTime());
});

test('charts have finite date bounds while filters load and for a single development point', () => {
  const empty = chartDateBounds([]);
  assert.ok(Number.isFinite(empty.min));
  assert.ok(Number.isFinite(empty.max));
  assert.equal(empty.max - empty.min, 90 * DAY_MS);
  const single = chartDateBounds([{ merge_commit_date: '2026-10-21' }]);
  assert.equal(single.max - single.min, DAY_MS);
});

test('date axes use about ten evenly spaced whole-day intervals', () => {
  const start = new Date('2026-07-21T00:00:00Z').getTime();
  const end = new Date('2026-10-21T00:00:00Z').getTime();

  assert.equal(dateAxisInterval(start, end), 10 * DAY_MS);
  assert.equal(dateAxisInterval(start, start + 5 * DAY_MS), DAY_MS);
});

test('timings below one second use milliseconds', () => {
  const unit = timingUnit([0.125, 0.95]);

  assert.equal(unit.label, 'ms');
  assert.equal(formatTimingTick(0.125, unit), '125');
});

test('timings at or above one second use seconds', () => {
  const unit = timingUnit([0.4, 1]);

  assert.equal(unit.label, 'sec');
  assert.equal(formatTimingTick(0.4, unit), '0.4');
});

test('invalid timing values do not force milliseconds', () => {
  const unit = timingUnit([null, Number.NaN, undefined]);

  assert.equal(unit.label, 'sec');
});

test('axis maxima are rounded up to readable boundaries', () => {
  assert.equal(niceAxisMax(0.19948), 0.2);
  assert.equal(niceAxisMax(0.55862), 0.6);
  assert.equal(niceAxisMax(1.08), 1.2);
});

test('release baselines use the newest patch from the two newest minor lines', () => {
  assert.deepEqual(
    latestReleaseVersions(['v1.4.5', 'v1.5.5', 'v2.0.0']),
    ['v2.0.0', 'v1.5.5']
  );
  assert.deepEqual(
    latestReleaseVersions(['v1.4.5', 'v1.5.5', 'v2.0.0', 'v2.0.1']),
    ['v2.0.1', 'v1.5.5']
  );
  assert.deepEqual(
    latestReleaseVersions(['v1.5.5', 'v2.0.0', 'v2.0.1', 'v2.1.0']),
    ['v2.1.0', 'v2.0.1']
  );
});

test('release baseline selection uses numeric versions and excludes prereleases', () => {
  assert.deepEqual(
    latestReleaseVersions(['v1.9.9', 'v1.10.0', 'v2.0.0-alpha1', 'development', 'v2.0']),
    ['v1.10.0', 'v1.9.9']
  );
});

test('patch releases keep the colour of their minor release line', () => {
  assert.equal(baselineColor('v2.0.1'), baselineColor('v2.0.0'));
  assert.notEqual(baselineColor('v2.1.0'), baselineColor('v2.0.1'));
});
