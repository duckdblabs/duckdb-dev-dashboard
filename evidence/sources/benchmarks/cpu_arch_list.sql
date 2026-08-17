-- CPU architectures present in the lake; drives the cpu architecture filter.
-- cpu_arch_label, not cpu_arch: a NULL would be dropped by the page's IN filter.
select distinct cpu_arch_label from benchmark_geomean order by cpu_arch_label
