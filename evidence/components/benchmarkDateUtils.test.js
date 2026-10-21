import test from 'node:test';
import assert from 'node:assert/strict';

import { dateRangeUrlValue } from './benchmarkDateUtils.js';

test('URL dates undo Evidence date shifts east of UTC', () => {
  assert.equal(dateRangeUrlValue('2026-10-19', -120), '2026-10-20');
  assert.equal(dateRangeUrlValue('2026-12-31', -60), '2027-01-01');
});

test('URL dates stay unchanged west of UTC and at UTC', () => {
  assert.equal(dateRangeUrlValue('2026-10-20', 0), '2026-10-20');
  assert.equal(dateRangeUrlValue('2026-10-20', 300), '2026-10-20');
});

test('invalid or absent date values pass through', () => {
  assert.equal(dateRangeUrlValue('not-a-date', -120), 'not-a-date');
  assert.equal(dateRangeUrlValue(undefined, -120), undefined);
});
