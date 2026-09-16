<script context="module">
  export const evidenceInclude = true;
</script>

<script>
  import { Column, DataTable } from '@evidence-dev/core-components';

  export let data;
</script>

<DataTable {data} rows=20 search=true>
    <Column id=query />
    <Column id='median (s)' />
    <Column id='v1.5.5 (s)' />
    <Column id='v1.4.5 (s)' />
    <Column
        id='ratio vs v1.5.5'
        fmt=num2
        contentType=colorscale
        scaleColumn='v1.5.5 color'
        colorScale={[['#dcfce7', '#14532d'], ['#fee2e2', '#7f1d1d']]}
        colorBreakpoints={[-1, 1]}
        colorMin={-1}
        colorMax={1}
    />
    <Column
        id='ratio vs v1.4.5'
        fmt=num2
        contentType=colorscale
        scaleColumn='v1.4.5 color'
        colorScale={[['#dcfce7', '#14532d'], ['#fee2e2', '#7f1d1d']]}
        colorBreakpoints={[-1, 1]}
        colorMin={-1}
        colorMax={1}
    />
    <Column id='# warm runs' />
    <Column id=status />
</DataTable>
