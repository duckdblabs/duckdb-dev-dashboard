<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import BenchmarkQueryChart from './BenchmarkQueryChart.svelte';

  export let data = [];
  export let baselines = [];
  export let dateStart;
  export let dateEnd;

  let search = '';
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

  $: queries = [...new Set((data ?? []).map((row) => row.query))].sort(collator.compare);
  $: needle = search.trim().toLowerCase();
  $: visibleQueries = needle
    ? queries.filter((query) => query.toLowerCase().includes(needle))
    : queries;
</script>

<div class="mb-4 flex flex-wrap items-end justify-between gap-3">
  <label class="block">
    <span class="mb-1 block text-sm text-base-content-muted">Find query</span>
    <input
        bind:value={search}
        type="search"
        placeholder="e.g. q42"
        class="h-8 w-56 rounded-md border border-base-300 bg-base-100 px-3 text-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-base-300"
    />
  </label>
  <span class="text-xs text-base-content-muted">Showing {visibleQueries.length} of {queries.length} queries</span>
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
      />
    {/each}
  </div>
{:else if queries.length > 0}
  <p class="text-sm text-base-content-muted">No query names match “{search}”.</p>
{/if}
