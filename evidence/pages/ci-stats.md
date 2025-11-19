---
title: CI metrics
---

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


### Time it takes for a job to start
Time in seconds

```sql wait_time
select * from wait_times;
```

<BarChart
    data={wait_time}
    x=date
    y=wait_time
/>

## Runtime

```sql runner_options
select distinct runner from run_jobs;
```

```sql repo_options
select distinct repo from run_jobs;
```

### Filters

<DateRange
    name=date_select
    defaultValue={'Last 30 Days'}
    title="Select time window"
    descritpion="Select time window"
/>
<br>
<Dropdown
    name=runner_select
    data={runner_options}
    value=runner
    selectAllByDefault=true
    multiple=true
    title="Select CI Runner OS"
    descritpion="Select CI Runner OS"
/>
<br>
<Dropdown
    name=repo_select
    data={repo_options}
    value=repo
    selectAllByDefault=true
    multiple=true
    title="Select repositories"
    descritpion="Select repositories"
/>


### Per runner

```sql runners_sql
select
  runner,
  count(*) as '# jobs',
  round(sum(epoch(completed_at - started_at)) / 60, 2) as 'total runtime (minutes)',
  round("total runtime (minutes)" / "# jobs", 2) as 'avg runtime'
from run_jobs
where completed_at > started_at
  and runner in ${inputs.runner_select.value}
  and repo in ${inputs.repo_select.value}
  and started_at between '${inputs.date_select.start}' and '${inputs.date_select.end}'
group by runner
order by runner;
```

<DataTable data={runners_sql}>
    <Column id=runner />
    <Column id='# jobs' />
    <Column id='total runtime (minutes)' />
    <Column id='avg runtime' />
</DataTable>


### Per repository

```sql repositories_sql
select
  repo as 'repository',
  count(*) as '# jobs',
  round(sum(epoch(completed_at - started_at)) / 60, 2) as 'total runtime (minutes)',
  round("total runtime (minutes)" / "# jobs", 2) as 'avg runtime'
from run_jobs
where completed_at > started_at
  and runner in ${inputs.runner_select.value}
  and repository in ${inputs.repo_select.value}
  and started_at between '${inputs.date_select.start}' and '${inputs.date_select.end}'
group by repo
order by repo;
```

<DataTable data={repositories_sql}>
    <Column id=repository />
    <Column id='# jobs' />
    <Column id='total runtime (minutes)' />
    <Column id='avg runtime' />
</DataTable>


### Per event

```sql events_sql
select
  event,
  count(*) as '# jobs',
  round(sum(epoch(completed_at - started_at)) / 60, 2) as 'total runtime (minutes)',
  round("total runtime (minutes)" / "# jobs", 2) as 'avg runtime'
from run_jobs
where completed_at > started_at
  and runner in ${inputs.runner_select.value}
  and repo in ${inputs.repo_select.value}
  and started_at between '${inputs.date_select.start}' and '${inputs.date_select.end}'
group by event
order by event;
```

<DataTable data={events_sql}>
    <Column id=event />
    <Column id='# jobs' />
    <Column id='total runtime (minutes)' />
    <Column id='avg runtime' />
</DataTable>


### Per workflow

```sql workflow_sql
select
  workflow_name,
  count(*) as '# jobs',
  round(sum(epoch(completed_at - started_at)) / 60, 2) as 'total runtime (minutes)',
  round("total runtime (minutes)" / "# jobs", 2) as 'avg runtime'
from run_jobs
where completed_at > started_at
  and runner in ${inputs.runner_select.value}
  and repo in ${inputs.repo_select.value}
  and started_at between '${inputs.date_select.start}' and '${inputs.date_select.end}'
group by workflow_name
order by workflow_name;
```

<DataTable data={workflow_sql}>
    <Column id=workflow_name />
    <Column id='# jobs' />
    <Column id='total runtime (minutes)' />
    <Column id='avg runtime' />
</DataTable>


### Per job

```sql job_sql
select
  job_name[:60] as job_name,
  count(*) as '# jobs',
  round(sum(epoch(completed_at - started_at)) / 60, 2) as 'total runtime (minutes)',
  round("total runtime (minutes)" / "# jobs", 2) as 'avg runtime'
from run_jobs
where completed_at > started_at
  and runner in ${inputs.runner_select.value}
  and repo in ${inputs.repo_select.value}
  and started_at between '${inputs.date_select.start}' and '${inputs.date_select.end}'
group by job_name
order by job_name;
```

<DataTable data={job_sql}>
    <Column id=job_name />
    <Column id='# jobs' />
    <Column id='total runtime (minutes)' />
    <Column id='avg runtime' />
</DataTable>
