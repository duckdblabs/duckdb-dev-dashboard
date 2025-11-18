-- Run and Job properties (selection)
select
  runs.repository['full_name'] as repo,
  runs.event as event,
  jobs.name as job_name,
  jobs.labels as runner,
  jobs.completed_at as completed_at,
  jobs.started_at as started_at,
  jobs.workflow_name as workflow_name,
from ci_jobs jobs
  join ci_runs runs on jobs.run_id = runs.id
  join ci_repositories repos on runs.repository['full_name'] = repos.full_name
where jobs.completed_at > jobs.started_at
  and not repos.private
