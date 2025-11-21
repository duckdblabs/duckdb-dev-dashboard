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

    def execute(self, sql_str, parameters=None):
        return self.con.execute(sql_str, parameters)

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

    def create_table(
        self,
        table_name: str,
        records: list[OrderedDict],
        or_replace: bool = False,
        if_not_exists: bool = False,
        with_no_data: bool =False,
    ):
        json_str = f"[{',\n'.join([json.dumps(rec) for rec in records])}]"
        # work-around: use temp-file to utilize the type-sniffer
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as tmp:
            tmp.write(json_str)
            tmp.flush()
            self.con.execute(
                f"""
                create
                {" or replace " if or_replace else ''}
                table
                {" if not exists " if if_not_exists else ''}
                {table_name}
                as from read_json('{tmp.name}')
                {" with no data " if with_no_data else ''}
                """
            )

    def append_table(self, table_name: str, records: list[OrderedDict]):
        json_str = f"[{',\n'.join([json.dumps(rec) for rec in records])}]"
        # work-around: use temp-file to utilize the type-sniffer
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as tmp:
            tmp.write(json_str)
            tmp.flush()
            self.con.execute(f"insert into {table_name} from read_json('{tmp.name}')")

    def upsert_table(
        self, table_name: str, records: list[dict], match_columns: list[str] = ['id'], print_changes: bool = False
    ):
        json_str = f"[{',\n'.join([json.dumps(rec) for rec in records])}]"
        # work-around: use temp-file to utilize the type-sniffer
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as tmp:
            tmp.write(json_str)
            tmp.flush()
            subquery = f"select * from read_json('{tmp.name}')"
            match_str = " and ".join([f"{table_name}.{attr} = upserts.{attr}" for attr in match_columns])
            self.execute(
                f"""
                merge into {table_name}
                using ({subquery}) as upserts
                on ({match_str})
                when matched then update
                when not matched then insert;
                """
            )
        if print_changes:
            # NOTE: we assume a new snapshot is created after every 'merge into', even if there are no changes, see: https://github.com/duckdblabs/duckdb-internal/issues/6557
            current_snapshot = self.current_snapshot()
            rel = self.table_changes(table_name, current_snapshot, current_snapshot)
            if rel.fetchone():
                print(f"new or updated records in {table_name}:")
                rel.show()
            else:
                print('no updates')

    def current_snapshot(self) -> int:
        return self.con.sql(f"from {self.ducklake_db_alias}.current_snapshot()").fetchone()[0]

    def table_changes(self, tbl: str, snapshot_start: int, snapshot_end: int) -> duckdb.DuckDBPyRelation:
        # returns new or updated records
        return self.con.sql(
            f"""
            select * exclude (snapshot_id, rowid, change_type)
              from {self.ducklake_db_alias}.table_changes('{tbl}', {snapshot_start}, {snapshot_end})
              where change_type = 'insert'
            union
            select * exclude (snapshot_id, rowid, change_type)
              from {self.ducklake_db_alias}.table_changes('{tbl}', {snapshot_start}, {snapshot_end})
              where change_type = 'update_postimage'
            except
            select * exclude (snapshot_id, rowid, change_type)
              from {self.ducklake_db_alias}.table_changes('{tbl}', {snapshot_start}, {snapshot_end})
              where change_type = 'update_preimage'
            except
            select * exclude (snapshot_id, rowid, change_type)
              from {self.ducklake_db_alias}.table_changes('{tbl}', {snapshot_start}, {snapshot_end})
              where change_type = 'delete'
            order by id;
            """
        )


# example usage:
# with DuckLakeConnection() as con:
#     con.sql('show tables').show()
