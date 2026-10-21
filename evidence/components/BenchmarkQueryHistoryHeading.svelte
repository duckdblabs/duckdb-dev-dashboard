<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  export let suite;
  export let dateStart;
  export let dateEnd;

  const suiteLabels = {
    'tpcds @ sf100': 'TPC-DS @ sf100',
    'tpch @ sf100': 'TPC-H @ sf100',
    clickbench: 'ClickBench'
  };

  // DateRange currently emits each selected calendar day as the preceding UTC date.
  // Add the same compensating day used by the page queries before presenting the range.
  const displayDate = (value) => {
    const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(value ?? ''));
    if (!match) return String(value ?? '');

    const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]) + 1));
    return new Intl.DateTimeFormat('en', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      timeZone: 'UTC'
    }).format(date);
  };

  $: suiteLabel = suiteLabels[suite] ?? suite ?? 'the selected benchmark suite';
  $: timeWindow = dateStart && dateEnd
    ? `${displayDate(dateStart)} and ${displayDate(dateEnd)}`
    : 'the selected dates';
</script>

<div id="query-history" class="mb-4 flex scroll-mt-20 items-center gap-2">
  <h2 class="m-0 text-xl font-semibold">
    <a
        href="#query-history"
        title="Link to query history"
        class="group inline-flex items-baseline gap-1 text-inherit no-underline"
    >
      History of {suiteLabel} between {timeWindow}
      <span aria-hidden="true" class="text-base-content-muted group-hover:text-primary">#</span>
    </a>
  </h2>
  <div class="group relative flex-none">
    <button
        type="button"
        class="flex h-5 w-5 items-center justify-center rounded-full border border-base-300 text-xs font-semibold text-base-content-muted hover:bg-base-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-base-300"
        aria-label="About query performance"
        aria-describedby="query-history-description"
    >
      i
    </button>
    <span id="query-history-description" class="sr-only">
      The query plots show the warm-run mean used to calculate the suite geomean. Median and
      fastest–slowest timings are included in each point's tooltip as noise context.
    </span>
    <span
        aria-hidden="true"
        class="pointer-events-none invisible absolute left-1/2 top-full z-20 mt-2 w-80 -translate-x-1/2 rounded-md border border-base-300 bg-base-100 p-2 text-left text-xs font-normal leading-relaxed text-base-content opacity-0 shadow-md transition-opacity group-hover:visible group-hover:opacity-100 group-focus-within:visible group-focus-within:opacity-100"
    >
      The query plots show the warm-run mean used to calculate the suite geomean. Median and
      fastest–slowest timings are included in each point's tooltip as noise context.
    </span>
  </div>
</div>
