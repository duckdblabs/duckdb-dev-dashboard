---
title: Extension Downloads
---

```sql extensions
  select
      extension_name as extension
  from extension_downloads.extension_downloads
  group by extension
  order by extension
```
<Dropdown data={extensions} name=extension value=extension>
    <DropdownOption value="%" valueLabel="All Extensions"/>
</Dropdown>

<Dropdown name=year>
    <DropdownOption value=2025/>
    <DropdownOption value=2024/>
</Dropdown>


```sql extension_downloads
  select
      week,
      downloads,
      extension_name as extension
  from extension_downloads.extension_downloads
  where year = ${inputs.year.value}
  and extension like '${inputs.extension.value}'
  group by all
  order by extension desc
```

<BarChart
    data={extension_downloads}
    title="Extension Downloads per week, {inputs.extension.label}"
    x=week
    y=downloads
    series=extension
/>
