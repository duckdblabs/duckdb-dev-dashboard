<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import BenchmarkSuiteChart from './BenchmarkSuiteChart.svelte';

  export let suites = [];
  export let data = [];
  export let baselines = [];
  export let failures = [];
  export let storage;
  export let platform;
  export let version;
  export let dateStart;
  export let dateEnd;
</script>

{#each suites ?? [] as suite (suite.benchmark_series)}
  <h2 class="mb-4 mt-8 text-xl font-semibold">{suite.suite_label}</h2>
  <BenchmarkSuiteChart
      {data}
      {baselines}
      {failures}
      series={suite.benchmark_series}
      {storage}
      {platform}
      {version}
      {dateStart}
      {dateEnd}
  />
{/each}
