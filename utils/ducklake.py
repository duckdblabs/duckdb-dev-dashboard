from collections import OrderedDict
import duckdb
import json
import tempfile


class DuckLakeConnection:
    def __init__(self):
        self.ducklake_db_alias = 'my_ducklake'

    def __enter__(self):
        self.con = duckdb.connect()
        self.con.execute(f"ATTACH 'ducklake:ducklake_secret' AS {self.ducklake_db_alias}")
        self.con.execute(f"USE {self.ducklake_db_alias}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.con.close()

    def sql(self, sql_str):
        return self.con.sql(sql_str)

    def execute(self, sql_str):
        return self.con.execute(sql_str)

    def fetchone(self):
        return self.con.fetchone()

    def fetchall(self):
        return self.con.fetchall()

    def table_exists(self, table_name: str) -> bool:
        return self.con.sql(
            f"select 1 from duckdb_tables() where table_name='{table_name}' and database_name='{self.ducklake_db_alias}'"
        ).fetchone() == (1,)

    def table_empty(self, table_name: str) -> bool:
        return self.con.sql(f"select count(*) from {table_name}").fetchone() == (0,)

    def max_id(self, table_name: str):
        return self.con.sql(f"select max(id) from {table_name}").fetchone()[0]

    def create_table(self, table_name: str, records: list[OrderedDict]):
        json_str = f"[{',\n'.join([json.dumps(rec) for rec in records])}]"
        # work-around: use temp-file to utilize the type-sniffer
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as tmp:
            tmp.write(json_str)
            tmp.flush()
            self.con.execute(f"create table {table_name} as from read_json('{tmp.name}')")

    def append_table(self, table_name: str, records: list[OrderedDict]):
        json_str = f"[{',\n'.join([json.dumps(rec) for rec in records])}]"
        # work-around: use temp-file to utilize the type-sniffer
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as tmp:
            tmp.write(json_str)
            tmp.flush()
            self.con.execute(f"insert into {table_name} from read_json('{tmp.name}')")

    def current_snapshot(self):
        return self.con.sql(f"from {self.ducklake_db_alias}.current_snapshot()").fetchone()[0]

    def table_changes(self, tbl, snapshot_start, snapshot_end):
        # returns new or updated records
        return self.con.sql(f"""
                            select * exclude (snapshot_id, rowid, change_type)
                              from {self.ducklake_db_alias}.table_changes('{tbl}', {snapshot_start}, {snapshot_end})
                              where change_type = 'update_postimage'
                            except
                            select * exclude (snapshot_id, rowid, change_type)
                              from {self.ducklake_db_alias}.table_changes('{tbl}', {snapshot_start}, {snapshot_end})
                              where change_type = 'update_preimage'
                            order by id;
                        """)


# example usage:
# with DuckLakeConnection() as con:
#     con.sql('show tables').show()
