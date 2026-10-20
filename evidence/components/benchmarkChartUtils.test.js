import test from 'node:test';
import assert from 'node:assert/strict';

import { formatTimingTick, timingUnit } from './benchmarkChartUtils.js';

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
