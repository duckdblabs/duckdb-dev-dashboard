<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { fullTimestamp, isCommitSha, shortDate } from './benchmarkChartUtils.js';
  import { benchmarkFailureHistory } from './benchmarkFailureUtils.js';

  export let data = [];

  $: runs = benchmarkFailureHistory(data ?? []);
  $: failureCount = runs.reduce((count, run) => count + run.failures.length, 0);
</script>

{#if runs.length > 0}
  <details id="benchmark-query-failures" class="mb-6 rounded-md border border-base-300 bg-base-100 p-3">
    <summary class="cursor-pointer text-sm text-base-content-muted">
      {failureCount} query {failureCount === 1 ? 'failure' : 'failures'} in
      {runs.length} benchmark {runs.length === 1 ? 'run' : 'runs'} ·
      last {shortDate(runs[0].merge_commit_date)}
    </summary>
    <p class="my-3 text-sm text-base-content-muted">
      These benchmark runs completed some queries. Each entry lists the queries that failed
      and their recorded errors. Failed queries have no timing and are excluded from the
      geometric mean; red × markers show their commit dates in the query charts below.
    </p>
    <ul class="m-0 list-none space-y-4 p-0">
      {#each runs as run}
        <li class="benchmark-failure-run border-t border-base-300 pt-3">
          <div class="text-sm font-medium">
            Merged {fullTimestamp(run.merge_commit_date)} · {run.duckdb_version}
            {#if isCommitSha(run.commit_sha)}
              · <a
                  class="underline"
                  href={`https://github.com/duckdb/duckdb/commit/${run.commit_sha}`}
                  target="_blank"
                  rel="noopener noreferrer"
              >{run.commit ?? run.commit_sha.slice(0, 8)}</a>
            {/if}
          </div>
          <p class="my-1 text-xs text-base-content-muted">
            Run {fullTimestamp(run.run_timestamp)} · {run.successfulCount} of
            {run.queryCount} queries succeeded; {run.failures.length} failed.
          </p>
          <ul class="m-0 list-none space-y-2 p-0">
            {#each run.failures as failure}
              <li class="flex gap-3 text-sm">
                <a class="shrink-0 font-medium underline" href={`#query-${failure.query}`}>
                  {failure.query}
                </a>
                <span class="min-w-0 break-words">
                  {failure.error ?? 'No error was recorded.'}
                </span>
              </li>
            {/each}
          </ul>
        </li>
      {/each}
    </ul>
  </details>
{/if}
