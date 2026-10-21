-- Shared suite discovery and display labels for overview charts and query-history dropdowns.
-- Keep storage in the key: a new suite can arrive on one backend before the other.
select distinct
  storage_type,
  benchmark_series,
  benchmark,
  scale_factor,
  case benchmark
    when 'tpcds' then 'TPC-DS'
    when 'tpch' then 'TPC-H'
    when 'clickbench' then 'ClickBench'
    when 'h2oai' then 'H2O.ai'
    else benchmark
  end || case
    when scale_factor is not null then ' @ sf' || printf('%g', scale_factor)
    when benchmark = 'h2oai' then ' (1 billion rows)'
    else ''
  end as suite_label,
  case benchmark
    when 'tpcds' then 1
    when 'tpch' then 2
    when 'clickbench' then 3
    when 'h2oai' then 4
    else 5
  end as suite_order
from benchmark_geomean
order by storage_type, suite_order, suite_label
