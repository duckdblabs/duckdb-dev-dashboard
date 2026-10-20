import test from 'node:test';
import assert from 'node:assert/strict';
import { createBenchmarkDisplayQuery, initialBenchmarkSuite } from './benchmarkSuiteUtils.js';

const suites = ['clickbench', 'h2oai', 'tpcds @ sf100', 'future-suite @ sf10']
  .map((benchmark_series) => ({ benchmark_series }));

test('query-history links accept H2O.ai and future suites discovered in the data', () => {
  assert.equal(initialBenchmarkSuite('h2oai', suites), 'h2oai');
  assert.equal(initialBenchmarkSuite('future-suite @ sf10', suites), 'future-suite @ sf10');
});

test('unknown or unsafe URL suite values fall back to the existing default', () => {
  for (const requested of [null, undefined, '', 'absent', "h2oai'; drop table runs; --"]) {
    assert.equal(initialBenchmarkSuite(requested, suites), 'tpcds @ sf100');
  }
});

test('a storage backend without TPC-DS defaults to its first available suite', () => {
  assert.equal(initialBenchmarkSuite(null, [{ benchmark_series: 'h2oai' }]), 'h2oai');
  assert.equal(initialBenchmarkSuite('h2oai', []), 'tpcds @ sf100');
});

test('URL-selected query displays fetch actual SQL without using prerendered rows', () => {
  let fetches = 0;
  const display = createBenchmarkDisplayQuery((text, options) => {
    assert.equal(options.disableCache, true);
    return { text, fetch: () => fetches++, rows: ['groupby_q01'] };
  });
  const hydrated = {
    id: 'query_history', text: "select * where benchmark = 'h2oai'",
    opts: {}, rows: ['q01']
  };
  const fresh = display(hydrated, true);
  assert.deepEqual(fresh.rows, ['groupby_q01']);
  assert.equal(fetches, 1);
  assert.equal(display(hydrated, true), fresh);
  assert.equal(fetches, 1);
});

test('changing filters replaces the displayed query for that dataset', () => {
  const fetched = [];
  const display = createBenchmarkDisplayQuery((text) => ({
    text, fetch: () => fetched.push(text)
  }));
  const source = { id: 'query_history', text: 'first selection', opts: {} };
  const first = display(source, true);
  const second = display({ ...source, text: 'next selection' }, true);
  assert.notEqual(second, first);
  assert.deepEqual(fetched, ['first selection', 'next selection']);
});

test('prerendering and unresolved inputs do not trigger a client query', () => {
  const display = createBenchmarkDisplayQuery(() => assert.fail('Query must not run'));
  const source = { id: 'query_history', text: 'select 1', opts: {} };
  assert.equal(display(source, false), source);
  const unresolved = { ...source, opts: { noResolve: true } };
  assert.equal(display(unresolved, true), unresolved);
  assert.equal(display(undefined, true), undefined);
});
