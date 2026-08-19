-- Real scale factors only, for the scale-factor filter.
--
-- Benchmarks with no scale factor (clickbench, scale_factor NULL) are deliberately excluded: an
-- 'n/a' entry in the dropdown is not a scale factor and there is nothing useful to pick. The page
-- compensates by exempting NULL-scale-factor runs from the filter entirely, so excluding them
-- here narrows the dropdown without hiding their data.
select distinct scale_factor_label
from benchmark_geomean
where scale_factor is not null
order by scale_factor_label
