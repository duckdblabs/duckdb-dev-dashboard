<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { Column, DataTable } from '@evidence-dev/core-components';

  export let data;
</script>

<DataTable {data} rows=25 search=true>
    <Column id=benchmark_series />
    <Column id=merge_date />
    <Column id=duckdb_version />
    <Column id=commit />
    <Column id='geomean (sec)' fmt=num3 />
    <Column id='# ok' />
    <Column id='# failed' />
    <Column id=platform />
    <Column id=query_set />
</DataTable>
