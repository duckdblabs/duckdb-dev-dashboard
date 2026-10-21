<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import BenchmarkSuiteChart from './BenchmarkSuiteChart.svelte';
  import { benchmarkSuiteAnchor } from './benchmarkSuiteUtils.js';

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
  <h2
      id={benchmarkSuiteAnchor(suite.suite_label)}
      class="markdown mb-4 mt-8 scroll-mt-[6.25rem] text-xl font-semibold"
  >
    <a href={`#${benchmarkSuiteAnchor(suite.suite_label)}`}>{suite.suite_label}</a>
  </h2>
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
