ATTACH 'ducklake:ducklake_secret' AS my_ducklake;
USE my_ducklake;

-- https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance
CHECKPOINT;
