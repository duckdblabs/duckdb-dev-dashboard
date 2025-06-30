## CI metrics

CI metrics from `duckdb/duckdb`

### Daily CI runs (finished only)
```sql daily_runs
select date_trunc('day', created_at) date, count(*) nr_runs
from all_runs
group by date_trunc('day', created_at),
order by date_trunc('day', created_at);
```

<BarChart
    data={daily_runs}
    x=date
    y=nr_runs
/>
