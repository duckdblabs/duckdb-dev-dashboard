<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { addBasePath } from '@evidence-dev/sdk/utils/svelte';

  export let storage;
  export let series;
  export let platform;
  export let start;
  export let end;
  export let version = undefined;

  $: params = new URLSearchParams({
    suite: series ?? '',
    platform: platform ?? '',
    start: start ?? '',
    end: end ?? '',
    ...(version ? { version } : {})
  });
  $: href = addBasePath(`/benchmarks/${storage}/queries?${params.toString()}`);
</script>

<a
    {href}
    class="inline-flex items-center rounded-md border border-base-300 bg-base-100 px-3 py-1.5 text-xs font-medium shadow-sm hover:bg-base-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-base-300"
>
  Explore query history →
</a>
