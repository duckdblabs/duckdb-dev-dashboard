<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  // Read-only companion to the platform dropdown, styled like it so the filter row reads as one
  // set of controls.
  export let memory = undefined;
</script>

<span class="inline-flex h-8 items-center whitespace-nowrap rounded-md border border-base-300 bg-base-100 px-3 text-xs shadow-sm">
  <span class="font-medium">Memory</span>
  <span class="mx-2 h-4 w-px bg-base-300"></span>
  <span class="text-base-content-muted">{memory ?? 'Unknown'}</span>
</span>
