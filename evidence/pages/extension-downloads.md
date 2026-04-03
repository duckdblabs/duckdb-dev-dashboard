---
title: Extension Downloads
---

```sql weeks_asc
  select unnest(generate_series(1, 52)) as week
```

```sql weeks_desc
  select unnest(generate_series(52, 1, -1)) as week
```

<Dropdown name=from_year title="From Year">
    <DropdownOption value=2026/>
    <DropdownOption value=2025/>
    <DropdownOption value=2024/>
</Dropdown>

<Dropdown data={weeks_desc} name=from_week value=week title="From Week"/>

<Dropdown name=to_year title="To Year">
    <DropdownOption value=2026/>
    <DropdownOption value=2025/>
    <DropdownOption value=2024/>
</Dropdown>

<Dropdown data={weeks_asc} name=to_week value=week title="To Week"/>

<Dropdown name=repository title="Repository">
    <DropdownOption value="%" valueLabel="All"/>
    <DropdownOption value="core" valueLabel="Core"/>
    <DropdownOption value="community" valueLabel="Community"/>
</Dropdown>

<Dropdown name=top_n title="Top N">
    <DropdownOption value={5} valueLabel="Top 5"/>
    <DropdownOption value={10} valueLabel="Top 10"/>
    <DropdownOption value={20} valueLabel="Top 20"/>
</Dropdown>

```sql top_n_data
  with top_ext as (
    select extension_name
    from extension_downloads.extension_downloads
    where (year * 100 + week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
      and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
    and repository like '${inputs.repository.value}'
    group by extension_name
    order by sum(downloads) desc
    limit ${inputs.top_n.value}
  ),
  weekly as (
    select
      e.year,
      e.week,
      (make_date(e.year::int, 1, 1) + interval ((e.week - 1) * 7) day)::date as period,
      e.extension_name as extension,
      sum(e.downloads) as downloads
    from extension_downloads.extension_downloads e
    inner join top_ext t on e.extension_name = t.extension_name
    where (e.year * 100 + e.week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
      and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
    and e.repository like '${inputs.repository.value}'
    group by all
  )
  select
    period,
    extension,
    downloads,
    sum(downloads) over (partition by extension order by year, week) as cumulative_downloads
  from weekly
  order by period, extension
```

<LineChart
    data={top_n_data}
    title="{inputs.top_n.label} — Weekly Downloads"
    x=period
    y=downloads
    series=extension
/>

<LineChart
    data={top_n_data}
    title="{inputs.top_n.label} — Cumulative Downloads"
    x=period
    y=cumulative_downloads
    series=extension
/>

## Single extension data

```sql extensions
  select extension_name as extension
  from extension_downloads.extension_downloads
  group by extension
  order by extension
```

<Dropdown data={extensions} name=extension value=extension title="Extension" defaultValue="ducklake"/>

```sql extension_detail
  with weekly as (
    select
      year,
      week,
      (make_date(year::int, 1, 1) + interval ((week::int - 1) * 7) day)::date as period,
      sum(downloads) as downloads
    from extension_downloads.extension_downloads
    where extension_name = '${inputs.extension.value}'
    and (year * 100 + week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
      and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
    and repository like '${inputs.repository.value}'
    group by year, week, period
  )
  select
    period,
    downloads,
    sum(downloads) over (order by year, week) as cumulative_downloads
  from weekly
  order by year, week
```

<LineChart
    data={extension_detail}
    title="{inputs.extension.value} — Weekly &amp; Cumulative Downloads"
    x=period
    y={["downloads", "cumulative_downloads"]}
/>

## Top N Weekly Downloads

```sql top_n_weekly_pivot
  with top_ext as (
    select extension_name
    from extension_downloads.extension_downloads
    where (year * 100 + week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
      and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
    and repository like '${inputs.repository.value}'
    group by extension_name
    order by sum(downloads) desc
    limit ${inputs.top_n.value}
  ),
  weekly as (
    select
      e.year,
      e.week,
      printf('%d-W%02d', e.year::int, e.week::int) as period,
      e.extension_name as extension,
      sum(e.downloads) as downloads
    from extension_downloads.extension_downloads e
    inner join top_ext t on e.extension_name = t.extension_name
    where (e.year * 100 + e.week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
      and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
    and e.repository like '${inputs.repository.value}'
    group by all
  ),
  weekly_sorted as (
    select * from weekly order by period
  )
  PIVOT weekly_sorted ON period USING sum(downloads) GROUP BY extension ORDER BY extension
```

<DataTable data={top_n_weekly_pivot}/>

## Top N Cumulative Downloads

```sql top_n_cumulative_pivot
  with top_ext as (
    select extension_name
    from extension_downloads.extension_downloads
    where (year * 100 + week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
      and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
    and repository like '${inputs.repository.value}'
    group by extension_name
    order by sum(downloads) desc
    limit ${inputs.top_n.value}
  ),
  weekly as (
    select
      e.year,
      e.week,
      printf('%d-W%02d', e.year::int, e.week::int) as period,
      e.extension_name as extension,
      sum(e.downloads) as downloads
    from extension_downloads.extension_downloads e
    inner join top_ext t on e.extension_name = t.extension_name
    where (e.year * 100 + e.week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
      and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
    and e.repository like '${inputs.repository.value}'
    group by all
  ),
  cumulative as (
    select
      period,
      extension,
      sum(downloads) over (partition by extension order by period) as cumulative_downloads
    from weekly
  ),
  cumulative_sorted as (
    select * from cumulative order by period
  )
  PIVOT cumulative_sorted ON period USING max(cumulative_downloads) GROUP BY extension ORDER BY extension
```

<DataTable data={top_n_cumulative_pivot}/>

## Raw data - Extension Downloads
Note: filters from the top of the page still apply!
```sql raw_downloads
  select
    year,
    week,
    extension_name as extension,
    downloads,
    repository,
    last_update
  from extension_downloads.extension_downloads
  where (year * 100 + week) between (${inputs.from_year.value} * 100 + ${inputs.from_week.value})
    and (${inputs.to_year.value} * 100 + ${inputs.to_week.value})
  and repository like '${inputs.repository.value}'
  order by year desc, week desc, extension, repository
```

<DataTable data={raw_downloads}>
    <Column id='year'/>
    <Column id='week'/>
    <Column id='extension'/>
    <Column id='downloads'/>
    <Column id='repository'/>
    <Column id='last_update' title='Last Update'/>
</DataTable>
