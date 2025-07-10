from pathlib import Path

from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.reader.reader import DeltaOps
from .reader import DataSourceReader
from typing import TYPE_CHECKING, Literal, Mapping, Optional, Sequence, Union
import sqlglot.expressions as ex
import sqlglot as sg

if TYPE_CHECKING:
    import pyarrow as pa
    from odbc2deltalake.metadata import InformationSchemaColInfo
    from deltalake import DeltaTable
    from deltalake.schema import PrimitiveType, MapType, ArrayType, StructType


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
        return ex.DataType.Type.NVARCHAR
    if pat.is_boolean(tp):
        return ex.DataType.Type.BIT
    if pat.is_int8(tp):
        return ex.DataType.Type.TINYINT
    if pat.is_int16(tp):
        return ex.DataType.Type.SMALLINT
    if pat.is_int32(tp):
        return ex.DataType.Type.INT
    if pat.is_int64(tp):
        return ex.DataType.Type.BIGINT
    if pat.is_float32(tp):
        return ex.DataType.Type.FLOAT
    if pat.is_float64(tp):
        return ex.DataType.Type.DOUBLE
    if pat.is_date32(tp):
        return ex.DataType.Type.DATE
    if pat.is_date64(tp):
        return ex.DataType.Type.DATETIME
    if pat.is_timestamp(tp):
        return ex.DataType.Type.DATETIME
    if pat.is_time32(tp):
        return ex.DataType.Type.TIME
    if pat.is_time64(tp):
        return ex.DataType.Type.TIME
    if pat.is_decimal(tp):
        return ex.DataType.Type.DECIMAL
    if pat.is_binary(tp):
        return ex.DataType.Type.VARBINARY
    if pat.is_fixed_size_binary(tp):
        return ex.DataType.build(f"binary({tp.byte_width})", dialect="tsql")
    raise ValueError(f"Type {tp} not supported")


def _build_type(t: Union[ex.DataType, ex.DataType.Type]):
    if isinstance(t, ex.DataType):
        return t
    return ex.DataType(this=t)


def _delta_to_sq_type(
    delta_type: "PrimitiveType | MapType | ArrayType | StructType",
) -> ex.DataType:
    from deltalake.schema import PrimitiveType, MapType, ArrayType, StructType

    if isinstance(delta_type, PrimitiveType):
        type_map = {
            "string": ex.DataType.Type.NVARCHAR,
            "long": ex.DataType.Type.BIGINT,
            "integer": ex.DataType.Type.INT,
            "short": ex.DataType.Type.SMALLINT,
            "byte": ex.DataType.Type.TINYINT,
            "float": ex.DataType.Type.FLOAT,
            "double": ex.DataType.Type.DOUBLE,
            "boolean": ex.DataType.Type.BIT,
            "binary": ex.DataType.Type.VARBINARY,
            "date": ex.DataType.Type.DATE,
            "timestamp": ex.DataType.Type.DATETIME,
            "timestampNtz": ex.DataType.Type.DATETIME,
        }
        if delta_type.type in type_map:
            return ex.DataType.build(type_map[delta_type.type], dialect="spark")
        else:
            return ex.DataType.build(delta_type.type, dialect="spark")
    elif isinstance(delta_type, MapType):
        key_type = _delta_to_sq_type(delta_type.key_type)
        value_type = _delta_to_sq_type(delta_type.value_type)

        return ex.DataType(
            this=ex.DataType.Type.MAP, expressions=[key_type, value_type], nested=True
        )
    elif isinstance(delta_type, ArrayType):
        item_type = _delta_to_sq_type(delta_type.element_type)
        return ex.DataType(
            this=ex.DataType.Type.ARRAY, expressions=[item_type], nested=True
        )
    elif isinstance(delta_type, StructType):
        fields = [
            ex.DataType(
                this=ex.DataType.Type.STRUCT,
                expressions=[
                    ex.ColumnDef(
                        this=ex.to_identifier(f.name),
                        kind=_delta_to_sq_type(f.type),
                    ),
                ],
                nested=True,
            )
            for f in delta_type.fields
        ]
        return ex.DataType(
            this=ex.DataType.Type.STRUCT, expressions=fields, nested=True
        )
    else:
        raise ValueError(f"Unsupported Delta type: {type(delta_type)}")


class DeltaRSDeltaOps(DeltaOps):
    def __init__(self, delta_table: "DeltaTable"):
        self.delta_table = delta_table

    def version(self) -> int:
        return self.delta_table.version()

    def vacuum(self, retention_hours: Union[int, None] = None):
        self.delta_table.vacuum(retention_hours)

    def restore(self, target: int):
        self.delta_table.restore(target)

    def set_properties(self, props: dict[str, str]):
        self.delta_table.delete("1=0", custom_metadata=props)

    def get_property(self, key: str):
        return self.delta_table.metadata().configuration.get(key, None)

    def set_nullable(self, cols: Mapping[str, bool]):
        # not implemented properly for delta-rs
        for k, v in cols.items():
            if v:
                self.delta_table.alter.add_constraint({f"null_{k}": f"{k} IS NOT NULL"})
            else:
                self.delta_table.alter.drop_constraint(
                    f"null_{k}", raise_if_not_exists=False
                )

    def column_infos(self) -> "Sequence[InformationSchemaColInfo]":
        from odbc2deltalake.metadata import InformationSchemaColInfo

        fields = self.delta_table.schema().fields
        return [
            InformationSchemaColInfo(
                column_name=f.name,
                data_type_str=str(f.type),
                data_type=_delta_to_sq_type(f.type),
                is_nullable=f.nullable,
            )
            for f in fields
        ]

    def update_incremental(self):
        self.delta_table.update_incremental()


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
            if isinstance(dt, Path):
                from deltalake import DeltaTable

                dt = DeltaTable(dt)
            dt.load_as_version(version)
        duckdb_create_view_for_delta(self.duck_con, dt, view_name)

    def local_execute_sql_to_py(self, sql: ex.Query) -> list[dict]:
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)
        with self.duck_con.cursor() as cursor:
            cursor.execute(sql.sql(self.query_dialect))
            assert cursor.description is not None
            col_names = [desc[0] for desc in cursor.description]
            return [dict(zip(col_names, row)) for row in cursor.fetchall()]

    def local_register_view(self, sql: ex.Query, view_name: str):
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)
        self.duck_con.sql(
            f"CREATE OR REPLACE VIEW {view_name} AS {sql.sql(self.query_dialect)}"
        )

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
                if isinstance(dt, Path):
                    from deltalake import DeltaTable

                    dt = DeltaTable(dt)
                DeltaTable.version(dt)
                if extended_check:
                    return len(dt.schema().fields) > 0
                return True
            except TableNotFoundError:
                return False
        return False

    def _handle_schema_drift(
        self,
        delta_path: Destination,
        allow_schema_drift: Union[bool, Literal["new_only"]],
        mode: Literal["overwrite", "append"],
        input_schema,
    ):
        from deltalake.exceptions import TableNotFoundError

        dp, do = delta_path.as_path_options("object_store")
        schema_mode = None
        if allow_schema_drift == "new_only":
            try:
                dt = delta_path.as_delta_table()
                if isinstance(dt, Path):
                    from deltalake import DeltaTable

                    dt = DeltaTable(dt)
                existing_schema = dt.schema().fields
                existing_field_names = [f.name for f in existing_schema]
                new_schema = _all_nullable(input_schema)
                new_fields = [
                    new_schema.field(n)
                    for n in new_schema.names
                    if n not in existing_field_names
                ]

                if new_fields:
                    import pyarrow as pa

                    merge_schema_fields = list()
                    dt = delta_path.as_delta_table()
                    if isinstance(dt, Path):
                        from deltalake import DeltaTable

                        dt = DeltaTable(dt)
                    pas_schema = dt.schema().to_pyarrow()
                    for n in input_schema.names:
                        if n in existing_field_names:
                            merge_schema_fields.append(pas_schema.field(n))
                        else:
                            merge_schema_fields.append(input_schema.field(n))

                    return pa.schema(merge_schema_fields), "merge"
            except TableNotFoundError:
                pass
        elif allow_schema_drift:
            schema_mode = "overwrite" if mode == "overwrite" else "merge"
        return None, schema_mode

    def local_execute_sql_to_delta(
        self,
        sql: ex.Query,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
    ):
        import duckdb
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)

        with self.duck_con.cursor() as cur:
            cur.execute(sql.sql(self.query_dialect))
            dp, do = delta_path.as_path_options("object_store")
            batch_reader = cur.fetch_record_batch()
            schema = batch_reader.schema
            cast_schema, schema_mode = self._handle_schema_drift(
                delta_path, allow_schema_drift, mode, schema
            )
            if cast_schema:
                schema = cast_schema
                batch_reader = pa.RecordBatchReader.from_batches(
                    cast_schema, (b.cast(cast_schema) for b in batch_reader)
                )
            try:
                write_deltalake(
                    dp,
                    batch_reader,
                    mode=mode,
                    schema_mode=schema_mode,
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

    def source_schema_limit_one(
        self, sql: ex.Query
    ) -> "list[InformationSchemaColInfo]":
        from ..metadata import InformationSchemaColInfo

        limit_sql = sql.limit(0).sql("tsql")
        from arrow_odbc import read_arrow_batches_from_odbc

        sc = read_arrow_batches_from_odbc(
            limit_sql, self.connection_string, max_text_size=20000
        ).schema
        return [
            InformationSchemaColInfo(
                column_name=n,
                data_type_str=str(sc.field(n).type),
                data_type=_build_type(_get_type(sc.field(n).type)),
                is_nullable=sc.field(n).nullable,
            )
            for n in sc.names
        ]

    def source_sql_to_py(self, sql: Union[str, ex.Query]) -> list[dict]:
        if isinstance(sql, ex.Query):
            sql = sql.sql("tsql")
        from arrow_odbc import read_arrow_batches_from_odbc

        result = list()
        for batch in read_arrow_batches_from_odbc(
            sql, self.connection_string, max_text_size=20000
        ):
            result.extend(batch.to_pylist())
        return result

    def source_write_sql_to_delta(
        self,
        sql: str,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
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
        cast_schema, schema_mode = self._handle_schema_drift(
            delta_path, allow_schema_drift, mode, reader.schema
        )

        if cast_schema:
            import pyarrow as pa

            reader = pa.RecordBatchReader.from_batches(
                cast_schema, (b.cast(cast_schema) for b in reader)
            )
        try:
            write_deltalake(
                dp,
                reader,
                schema=_all_nullable(reader.schema),
                mode=mode,
                writer_properties=self.writer_properties,
                schema_mode=schema_mode,
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
        if isinstance(dt, Path):
            from deltalake import DeltaTable

            dt = DeltaTable(dt)
        return DeltaRSDeltaOps(dt)

    def local_upsert_into(
        self,
        local_sql_source: ex.Query,
        target_delta: Destination,
        merge_cols: Sequence[str],
    ):
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)

        with self.duck_con.cursor() as cursor:
            cursor.execute(local_sql_source.sql(self.query_dialect))

            dt = target_delta.as_delta_table()
            if isinstance(dt, Path):
                from deltalake import DeltaTable

                dt = DeltaTable(dt)
            res = (
                dt.merge(
                    cursor.fetch_record_batch(),
                    sg.and_(
                        *[
                            sg.column(mc, "tgt", quoted=True).eq(
                                sg.column(mc, "src", quoted=True)
                            )
                            for mc in merge_cols
                        ]
                    ).sql(self.query_dialect),
                    "src",
                    "tgt",
                    writer_properties=self.writer_properties,
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            print(res)
