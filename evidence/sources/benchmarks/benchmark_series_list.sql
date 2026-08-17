-- one entry per chart on the benchmarks page: a benchmark at a scale factor
select distinct
  benchmark_series,
  benchmark,
  scale_factor
from benchmark_geomean
order by benchmark, scale_factor
