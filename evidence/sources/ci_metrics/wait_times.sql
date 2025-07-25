select created_at::date as date, AVG(EXTRACT(EPOCH FROM started_at - created_at))::int as wait_time
from ci_jobs
where started_at >= created_at
group by date
order by date
