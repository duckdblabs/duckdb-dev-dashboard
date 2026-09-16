<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { onMount } from 'svelte';
  import { page } from '$app/stores';
  import { replaceState } from '$app/navigation';
  import { getReadonlyInputContext } from '@evidence-dev/sdk/utils/svelte';

  const inputs = getReadonlyInputContext();

  onMount(() => inputs.subscribe((values) => {
    const suite = values.suite_select?.value;
    const platform = values.platform_select?.value;
    const start = values.date_select?.start;
    const end = values.date_select?.end;
    if (!suite || !platform || !start || !end) return;

    const url = new URL($page.url);
    url.searchParams.set('suite', suite);
    url.searchParams.set('platform', platform);
    url.searchParams.set('start', start);
    url.searchParams.set('end', end);
    if (url.href !== $page.url.href) replaceState(url, $page.state);
  }));
</script>
