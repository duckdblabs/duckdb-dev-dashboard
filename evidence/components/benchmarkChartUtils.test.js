import test from 'node:test';
import assert from 'node:assert/strict';

import {
  dateAxisInterval,
  formatTimingTick,
  niceAxisMax,
  timingUnit
} from './benchmarkChartUtils.js';

const DAY_MS = 24 * 60 * 60 * 1000;

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
