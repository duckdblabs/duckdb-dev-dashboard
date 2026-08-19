-- timings from different machines are not comparable, so the page filters on this.
-- machine_label, not machine_type: the latter is NULL unless the harness was run with
-- --machine-type, and an IN filter over NULLs would empty the dashboard.
select distinct machine_label from benchmark_geomean order by machine_label
