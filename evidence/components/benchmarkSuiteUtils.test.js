import test from 'node:test';
import assert from 'node:assert/strict';
import {
  benchmarkSuiteAnchor,
  createBenchmarkDisplayQuery,
  initialBenchmarkSuite
} from './benchmarkSuiteUtils.js';

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

test('dynamic overview headings preserve the existing suite anchors', () => {
  assert.equal(benchmarkSuiteAnchor('TPC-DS @ sf100'), 'tpc-ds--sf100');
  assert.equal(benchmarkSuiteAnchor('TPC-H @ sf100'), 'tpc-h--sf100');
  assert.equal(benchmarkSuiteAnchor('ClickBench'), 'clickbench');
  assert.equal(benchmarkSuiteAnchor('H2O.ai (1 billion rows)'), 'h2oai-1-billion-rows');
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

test('the top chart receives the requested suite on first load without a release-line click', () => {
  const prerendered = [{ benchmark_series: 'tpcds @ sf100', geomean_seconds: 1 }];
  const requested = [{ benchmark_series: 'tpch @ sf100', geomean_seconds: 2 }];
  const display = createBenchmarkDisplayQuery((text, options) => {
    const query = { text, rows: [] };
    query.fetch = () => { query.rows = options.disableCache ? requested : prerendered; };
    return query;
  });
  const initial = {
    id: 'geomean', text: "select * where benchmark_series = 'tpch @ sf100'",
    opts: {}, rows: prerendered
  };
  assert.deepEqual(display(initial, true).rows, requested);
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
