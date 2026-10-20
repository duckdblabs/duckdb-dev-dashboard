export const initialBenchmarkSuite = (requested, suites = []) => {
  // Only interpolate a URL value into page SQL after matching it to a discovered suite.
  if (suites.some((suite) => suite.benchmark_series === requested)) return requested;
  if (suites.some((suite) => suite.benchmark_series === 'tpcds @ sf100')) return 'tpcds @ sf100';
  return suites[0]?.benchmark_series ?? 'tpcds @ sf100';
};

export const createBenchmarkDisplayQuery = (createQuery) => {
  const queries = new Map();
  return (query, refresh = false) => {
    if (!refresh || !query || query.opts.noResolve) return query;
    // URL-selected SQL can be hydrated with rows prerendered for the default suite.
    // Bypass that shared cache and keep one fetched query per displayed dataset.
    const cached = queries.get(query.id);
    if (cached?.text === query.text) return cached;
    const fresh = createQuery(query.text, {
      id: `query-history-client-${query.id}`, disableCache: true
    });
    queries.set(query.id, fresh);
    fresh.fetch();
    return fresh;
  };
};
