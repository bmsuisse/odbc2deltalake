from pathlib import Path
from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.reader.reader import DeltaOps
from .reader import DataSourceReader
from typing import TYPE_CHECKING, Literal
from sqlglot.expressions import Query

if TYPE_CHECKING:
    import pyarrow as pa


def _all_nullable(schema: "pa.Schema") -> "pa.Schema":
    import pyarrow.types as pat

    sc = schema
    for i, n in enumerate(schema.names):
        f = schema.field(n)
        if not f.nullable:
            sc = sc.set(i, f.with_nullable(True))
    return sc


class ODBCReader(DataSourceReader):
    def __init__(self, connection_string: str) -> None:
        from deltalake import WriterProperties

        self.connection_string = connection_string
        self.writer_properties = WriterProperties(compression="ZSTD")
        self.duck_con = None
        pass

    def local_register_update_view(
        self, delta_path: Destination, view_name: str, *, version: int | None = None
    ):
        import duckdb
        from deltalake2db import duckdb_create_view_for_delta

        self.duck_con = self.duck_con or duckdb.connect()
        dt = delta_path.as_delta_table()
        if version is not None:
            dt.load_as_version(version)
        duckdb_create_view_for_delta(self.duck_con, dt, view_name)

    def local_execute_sql_to_py(self, sql: Query) -> list[dict]:
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect()
        with self.duck_con.cursor() as cursor:
            cursor.execute(sql.sql("duckdb"))
            assert cursor.description is not None
            col_names = [desc[0] for desc in cursor.description]
            return [dict(zip(col_names, row)) for row in cursor.fetchall()]

    def local_register_view(self, sql: Query, view_name: str):
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect()
        self.duck_con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {sql.sql('duckdb')}")

    def local_execute_sql_to_delta(
        self, sql: Query, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        import duckdb
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError

        self.duck_con = self.duck_con or duckdb.connect()

        with self.duck_con.cursor() as cur:
            cur.execute(sql.sql("duckdb"))
            dp, do = delta_path.as_path_options("object_store")
            batch_reader = cur.fetch_record_batch()
            schema = batch_reader.schema
            try:

                write_deltalake(
                    dp,
                    batch_reader,
                    mode=mode,
                    schema_mode="overwrite" if mode == "overwrite" else "merge",
                    writer_properties=self.writer_properties,
                    engine="rust",
                    storage_options=do,
                )
            except DeltaError as e:
                if "No data source supplied to write command" in str(e):
                    if mode == "overwrite":
                        self._write_empty_delta_table(schema, dp, do)
                else:
                    raise e

    @property
    def query_dialect(self) -> str:
        return "duckdb"

    def source_sql_to_py(self, sql: str | Query) -> list[dict]:
        if isinstance(sql, Query):
            sql = sql.sql("tsql")
        from arrow_odbc import read_arrow_batches_from_odbc

        result = list()
        for batch in read_arrow_batches_from_odbc(sql, self.connection_string):
            result.extend(batch.to_pylist())
        return result

    def source_write_sql_to_delta(
        self, sql: str, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        from arrow_odbc import read_arrow_batches_from_odbc
        from deltalake import DeltaTable, WriterProperties, write_deltalake
        from deltalake.exceptions import TableNotFoundError, DeltaError

        reader = read_arrow_batches_from_odbc(
            query=sql,
            connection_string=self.connection_string,
            max_binary_size=20000,
            max_text_size=20000,
        )
        dp, do = delta_path.as_path_options(flavor="object_store")
        try:
            write_deltalake(
                dp,
                reader,
                schema=_all_nullable(reader.schema),
                mode=mode,
                writer_properties=self.writer_properties,
                schema_mode="overwrite" if mode == "overwrite" else "merge",
                engine="rust",
                storage_options=do,
            )
        except DeltaError as e:
            if "No data source supplied to write command" in str(e):
                if mode == "overwrite":
                    self._write_empty_delta_table(reader.schema, dp, do)
            else:
                raise e

    def _write_empty_delta_table(
        self,
        schema: "pa.Schema",
        path: str | Path,
        storage_options: dict[str, str] | None,
    ):
        from deltalake import DeltaTable, WriterProperties, write_deltalake

        write_deltalake(
            path,
            [],
            schema=_all_nullable(schema),
            mode="overwrite",
            schema_mode="overwrite",
            engine="pyarrow",
            storage_options=storage_options,
        )

    def get_local_delta_ops(self, delta_path: Destination) -> DeltaOps:
        dt = delta_path.as_delta_table()
        return dt
