<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import BenchmarkQueryChart from './BenchmarkQueryChart.svelte';
  import {
    latestAnnotationVersion,
    scoreRecentlySlower,
    scoreReleaseSlower
  } from './benchmarkRegressionUtils.js';

  export let data = [];
  export let baselines = [];
  export let dateStart;
  export let dateEnd;

  let search = '';
  let filterMode = 'all';
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

  $: rowsByQuery = (data ?? []).reduce((grouped, row) => {
    if (!grouped.has(row.query)) grouped.set(row.query, []);
    grouped.get(row.query).push(row);
    return grouped;
  }, new Map());
  $: queries = [...rowsByQuery.keys()].sort(collator.compare);
  $: recentComparisons = Object.fromEntries(queries.map((query) => [
    query,
    scoreRecentlySlower(rowsByQuery.get(query))
  ]));
  $: annotationVersion = latestAnnotationVersion(baselines);
  $: annotationLabel = annotationVersion?.replace(/^v/, '') ?? 'release';
  $: annotationBaselines = Object.fromEntries((baselines ?? [])
    .filter((row) => row.duckdb_version === annotationVersion)
    .map((row) => [row.query, row.baseline_seconds]));
  $: releaseComparisons = Object.fromEntries(queries.map((query) => [
    query,
    scoreReleaseSlower(rowsByQuery.get(query), annotationBaselines[query], annotationVersion)
  ]));
  $: filterModes = [
    { value: 'all', label: 'All' },
    {
      value: 'release_slower',
      label: `Slower than ${annotationLabel}`,
      disabled: !annotationVersion,
      description: annotationVersion
        ? `Shows queries whose median over the latest 5 successful points is at least 10% slower than ${annotationVersion}. Baselines of 50 ms or less are ignored.`
        : 'No release annotation is available for the current filters.'
    },
    {
      value: 'recently_slower',
      label: 'Recently slower',
      description: 'Shows queries whose latest-3 median is at least 5% slower than the preceding-5 median. Preceding medians of 50 ms or less are ignored.'
    }
  ];
  $: needle = search.trim().toLowerCase();
  $: matchingQueries = needle
    ? queries.filter((query) => query.toLowerCase().includes(needle))
    : queries;
  $: selectedComparisons = filterMode === 'release_slower'
    ? releaseComparisons
    : recentComparisons;
  $: visibleQueries = filterMode === 'all'
    ? matchingQueries
    : matchingQueries
      .filter((query) => selectedComparisons[query]?.isSlower)
      .sort((left, right) =>
        selectedComparisons[right].percentChange - selectedComparisons[left].percentChange
        || collator.compare(left, right));
  $: cardComparisons = Object.fromEntries(queries.map((query) => {
    const comparison = filterMode === 'release_slower'
      ? releaseComparisons[query]
      : recentComparisons[query];
    return [query, comparison?.isSlower ? comparison : null];
  }));
</script>

<div class="mb-4 flex flex-wrap items-end justify-between gap-3">
  <div class="flex flex-wrap items-end gap-3">
    <label class="block">
      <span class="mb-1 block text-sm text-base-content-muted">Find query</span>
      <input
          bind:value={search}
          type="search"
          placeholder="e.g. q42"
          class="h-8 w-56 rounded-md border border-base-300 bg-base-100 px-3 text-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-base-300"
      />
    </label>

    <div class="block">
      <span class="mb-1 block text-sm text-base-content-muted">Show queries</span>
      <div
          class="inline-flex h-8 rounded-md border border-base-300 shadow-sm"
          role="group"
          aria-label="Show queries"
      >
        {#each filterModes as mode, index}
          <div class="group relative flex border-r border-base-300 last:border-r-0">
          <button
              type="button"
              class="flex-none px-3 py-1 text-xs font-medium hover:bg-base-200 focus:z-10 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-base-300 disabled:pointer-events-none disabled:opacity-50 {index === 0 ? 'rounded-l-md' : ''} {index === filterModes.length - 1 ? 'rounded-r-md' : ''} {filterMode === mode.value ? 'z-10 bg-base-200 text-primary' : 'z-0 bg-base-100'}"
              aria-pressed={filterMode === mode.value}
              aria-describedby={mode.description ? `query-filter-${mode.value}-description` : undefined}
              disabled={mode.disabled}
              on:click={() => filterMode = mode.value}
          >
            {mode.label}
          </button>
          {#if mode.description}
            <span id={`query-filter-${mode.value}-description`} class="sr-only">{mode.description}</span>
            <span
                aria-hidden="true"
                class="pointer-events-none invisible absolute left-1/2 top-full z-20 mt-2 w-72 -translate-x-1/2 rounded-md border border-base-300 bg-base-100 p-2 text-left text-xs font-normal leading-relaxed text-base-content opacity-0 shadow-md transition-opacity group-hover:visible group-hover:opacity-100 group-focus-within:visible group-focus-within:opacity-100"
            >
              {mode.description}
            </span>
          {/if}
          </div>
        {/each}
      </div>
    </div>
  </div>
  <span class="text-xs text-base-content-muted">
    {#if filterMode === 'release_slower'}
      Showing {visibleQueries.length} slower than {annotationLabel} of {queries.length} queries
    {:else if filterMode === 'recently_slower'}
      Showing {visibleQueries.length} recently slower of {queries.length} queries
    {:else}
      Showing {visibleQueries.length} of {queries.length} queries
    {/if}
  </span>
</div>

{#if visibleQueries.length > 0}
  <div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
    {#each visibleQueries as query (query)}
      <BenchmarkQueryChart
          {data}
          {baselines}
          {query}
          {dateStart}
          {dateEnd}
          comparison={cardComparisons[query]}
      />
    {/each}
  </div>
{:else if queries.length > 0}
  {#if filterMode === 'release_slower'}
    <p class="text-sm text-base-content-muted">No queries are at least 10% slower than {annotationLabel}.</p>
  {:else if filterMode === 'recently_slower'}
    <p class="text-sm text-base-content-muted">No recently slower queries match the current filters.</p>
  {:else}
    <p class="text-sm text-base-content-muted">No query names match “{search}”.</p>
  {/if}
{/if}
