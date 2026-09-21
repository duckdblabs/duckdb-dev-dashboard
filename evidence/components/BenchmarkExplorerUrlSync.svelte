<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { onMount } from 'svelte';
  import { page } from '$app/stores';
  import { replaceState } from '$app/navigation';
  import { getReadonlyInputContext } from '@evidence-dev/sdk/utils/svelte';

  const inputs = getReadonlyInputContext();

  const syncUrl = (values) => {
    const suite = values.suite_select?.value;
    const platform = values.platform_select?.value;
    const start = values.date_select?.start;
    const end = values.date_select?.end;
    if (!suite || !platform || !start || !end) return;

    // SvelteKit's shallow replaceState updates the address bar but intentionally leaves
    // $page.url unchanged. Use the real browser location so repeated Evidence input emissions do
    // not keep replacing the same URL until the browser rate-limits the History API.
    const url = new URL(window.location.href);
    url.searchParams.set('suite', suite);
    url.searchParams.set('platform', platform);
    url.searchParams.set('start', start);
    url.searchParams.set('end', end);
    if (url.href !== window.location.href) replaceState(url, $page.state);
  };

  onMount(() => {
    let unsubscribe;
    // Input stores emit synchronously when subscribed. Wait until the initial component mount has
    // returned so SvelteKit's router is initialized before that first emission can replace state.
    const timer = setTimeout(() => {
      unsubscribe = inputs.subscribe(syncUrl);
    }, 0);

    return () => {
      clearTimeout(timer);
      unsubscribe?.();
    };
  });
</script>
