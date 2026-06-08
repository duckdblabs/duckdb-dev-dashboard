ATTACH 'ducklake:ducklake_secret' AS my_ducklake;
USE my_ducklake;
show tables;
select updated_at from ci_repositories where name = 'duckdb';

-- CALL ducklake_merge_adjacent_files('my_ducklake');
-- CALL ducklake_cleanup_old_files('my_ducklake', cleanup_all => true);
