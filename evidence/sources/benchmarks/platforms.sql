-- Shared platform metadata for the storage benchmark pages.
--
-- The benchmark lake records the OS, architecture and machine type, but it does not currently
-- record enough hardware detail to build the compact display labels. Keep the curated mapping in
-- one source query so the DuckDB and DuckLake pages cannot drift apart.
select distinct
  storage_type,
  concat_ws('|', coalesce(os, 'unknown'), cpu_arch_label, machine_label) as platform_id,
  os,
  cpu_arch_label,
  machine_label,
  case
    when os = 'linux' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then 'Ubuntu 24.04 (amd64, 16 vCPU)'
    when os = 'linux' and cpu_arch_label = 'arm64' and machine_label = 'c7gd.4xlarge'
      then 'Ubuntu 24.04 (arm64, 16 vCPU)'
    when os = 'macos' and cpu_arch_label = 'arm64' and machine_label = 'mac-m4.metal'
      then 'macOS m4 (arm64, 10 vCPU)'
    when os = 'windows' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then 'Windows Server 2025 (amd64, 16 vCPU)'
    else
      (case coalesce(os, 'unknown')
        when 'linux' then 'Linux'
        when 'macos' then 'macOS'
        when 'windows' then 'Windows'
        else coalesce(os, 'Unknown OS')
      end)
      || ' ('
      || (case cpu_arch_label when 'x86_64' then 'amd64' else cpu_arch_label end)
      || ', '
      || (case machine_label when 'unspecified' then 'instance unspecified' else machine_label end)
      || ')'
  end as platform_label,
  case
    when os = 'linux' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then '32 GiB'
    when os = 'linux' and cpu_arch_label = 'arm64' and machine_label = 'c7gd.4xlarge'
      then '32 GiB'
    when os = 'macos' and cpu_arch_label = 'arm64' and machine_label = 'mac-m4.metal'
      then '24 GiB'
    when os = 'windows' and cpu_arch_label = 'x86_64' and machine_label = 'c6id.4xlarge'
      then '32 GiB'
    else 'Unknown'
  end as memory_label
from benchmark_geomean
where merge_commit_date is not null
order by storage_type, platform_label collate nocase, platform_label
