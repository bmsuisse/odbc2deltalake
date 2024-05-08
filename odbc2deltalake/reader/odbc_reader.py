from pathlib import Path

from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.reader.reader import DeltaOps
from .reader import DataSourceReader
from typing import TYPE_CHECKING, Literal, Optional, Union
from sqlglot.expressions import Query, DataType

if TYPE_CHECKING:
    import pyarrow as pa
    from odbc2deltalake.metadata import InformationSchemaColInfo


def _all_nullable(schema: "pa.Schema") -> "pa.Schema":
    sc = schema
    for i, n in enumerate(schema.names):
        f = schema.field(n)
        if not f.nullable:
            sc = sc.set(i, f.with_nullable(True))
    return sc


def _get_type(tp: "pa.DataType"):
    import pyarrow.types as pat

    if pat.is_string(tp):
        return DataType.Type.NVARCHAR
    if pat.is_boolean(tp):
        return DataType.Type.BIT
    if pat.is_int8(tp):
        return DataType.Type.TINYINT
    if pat.is_int16(tp):
        return DataType.Type.SMALLINT
    if pat.is_int32(tp):
        return DataType.Type.INT
    if pat.is_int64(tp):
        return DataType.Type.BIGINT
    if pat.is_float32(tp):
        return DataType.Type.FLOAT
    if pat.is_float64(tp):
        return DataType.Type.DOUBLE
    if pat.is_date32(tp):
        return DataType.Type.DATE
    if pat.is_date64(tp):
        return DataType.Type.DATETIME
    if pat.is_timestamp(tp):
        return DataType.Type.DATETIME
    if pat.is_time32(tp):
        return DataType.Type.TIME
    if pat.is_time64(tp):
        return DataType.Type.TIME
    if pat.is_decimal(tp):
        return DataType.Type.DECIMAL
    if pat.is_binary(tp):
        return DataType.Type.VARBINARY
    if pat.is_fixed_size_binary(tp):
        return DataType.build(f"binary({tp.byte_width})", dialect="tsql")
    raise ValueError(f"Type {tp} not supported")


def _build_type(t: Union[DataType, DataType.Type]):
    if isinstance(t, DataType):
        return t
    return DataType(this=t)


class ODBCReader(DataSourceReader):
    def __init__(self, connection_string: str, local_db: str = ":memory:") -> None:
        from deltalake import WriterProperties

        self.connection_string = connection_string
        self.writer_properties = WriterProperties(compression="ZSTD")
        self.duck_con = None
        self.local_db = local_db

    @property
    def supports_proc_exec(self):
        return True

    def local_register_update_view(
        self,
        delta_path: Destination,
        view_name: str,
        *,
        version: Union[int, None] = None,
    ):
        import duckdb
        from deltalake2db import duckdb_create_view_for_delta

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)
        dt = delta_path.as_delta_table()
        if version is not None:
            dt.load_as_version(version)
        duckdb_create_view_for_delta(self.duck_con, dt, view_name)

    def local_execute_sql_to_py(self, sql: Query) -> list[dict]:
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)
        with self.duck_con.cursor() as cursor:
            cursor.execute(sql.sql("duckdb"))
            assert cursor.description is not None
            col_names = [desc[0] for desc in cursor.description]
            return [dict(zip(col_names, row)) for row in cursor.fetchall()]

    def local_register_view(self, sql: Query, view_name: str):
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)
        self.duck_con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {sql.sql('duckdb')}")

    def local_pylist_to_delta(
        self,
        pylist: list[dict],
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        dummy_record: Union[dict, None] = None,
    ):
        from deltalake import write_deltalake
        import pyarrow as pa

        schema = (
            pa.RecordBatch.from_pylist([dummy_record]).schema if dummy_record else None
        )
        batch = pa.RecordBatch.from_pylist(pylist, schema=schema)
        dp, do = delta_path.as_path_options("object_store")

        write_deltalake(
            dp,
            [batch],
            storage_options=do,
            schema=batch.schema,
            mode=mode,
            schema_mode="overwrite" if mode == "overwrite" else "merge",
            writer_properties=self.writer_properties,
            engine="rust",
        )

    def local_delta_table_exists(
        self, delta_path: Destination, extended_check=False
    ) -> bool:
        if delta_path.exists():
            from deltalake import DeltaTable
            from deltalake.exceptions import TableNotFoundError

            try:
                dt = delta_path.as_delta_table()
                DeltaTable.version(dt)
                if extended_check:
                    return len(dt.schema().fields) > 0
                return True
            except TableNotFoundError:
                return False
        return False

    def local_execute_sql_to_delta(
        self, sql: Query, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        import duckdb
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)

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

    def source_schema_limit_one(self, sql: Query) -> "list[InformationSchemaColInfo]":
        from ..metadata import InformationSchemaColInfo

        limit_sql = sql.limit(0).sql("tsql")
        from arrow_odbc import read_arrow_batches_from_odbc

        sc = read_arrow_batches_from_odbc(
            limit_sql, self.connection_string, max_text_size=20000
        ).schema
        return [
            InformationSchemaColInfo(
                column_name=n,
                data_type=_build_type(_get_type(sc.field(n).type)),
                is_nullable=sc.field(n).nullable,
            )
            for n in sc.names
        ]

    def source_sql_to_py(self, sql: Union[str, Query]) -> list[dict]:
        if isinstance(sql, Query):
            sql = sql.sql("tsql")
        from arrow_odbc import read_arrow_batches_from_odbc

        result = list()
        for batch in read_arrow_batches_from_odbc(
            sql, self.connection_string, max_text_size=20000
        ):
            result.extend(batch.to_pylist())
        return result

    def source_write_sql_to_delta(
        self, sql: str, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        from arrow_odbc import read_arrow_batches_from_odbc
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError

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
        path: Union[str, Path],
        storage_options: Optional[dict[str, str]],
    ):
        from deltalake import write_deltalake

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
