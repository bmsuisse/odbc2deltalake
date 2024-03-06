from pathlib import Path
from typing import Iterable, Literal, Sequence, TypeVar
from deltalake import DeltaTable, WriterProperties, write_deltalake
from deltalake.exceptions import TableNotFoundError
from arrow_odbc import read_arrow_batches_from_odbc
import asyncio
from .sql_schema import get_sql_for_schema
from .query import sql_quote_name
import os
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyodbc
from .metadata import (
    get_primary_keys,
    get_columns,
    table_name_type,
    InformationSchemaColInfo,
)
import json
from deltalake2db import get_sql_for_delta_expr
import duckdb
import time
import sqlglot.expressions as ex
from .sql_glot_utils import table_from_tuple, union
from deltalake2db.sql_utils import read_parquet
import shutil

IS_DELETED_COL_NAME = "__is_deleted"
IS_DELETED_COL_INFO = InformationSchemaColInfo.from_name_type(
    IS_DELETED_COL_NAME, "bit"
)
VALID_FROM_COL_NAME = "__timestamp"
VALID_FROM_COL_INFO = InformationSchemaColInfo.from_name_type(
    VALID_FROM_COL_NAME, "datetimeoffset"
)
IS_FULL_LOAD_COL_NAME = "__is_full_load"
IS_FULL_LOAD_COL_INFO = InformationSchemaColInfo.from_name_type(
    IS_FULL_LOAD_COL_NAME, "bit"
)

T = TypeVar("T")


def _not_none(v: T | None) -> T:
    if v is None:
        raise ValueError("Value is None")
    return v


class DBDeltaPathConfigs:
    DELTA_1_NAME = "delta_1"
    """common data for delta load contains data that has changed after last full load via naive criteria timestamp > last_timestamp (or similar, can also be dates)"""
    DELTA_2_NAME = "delta_2"
    """delta 2 is data where timestamp is different for whatever reason, eg restore"""
    PRIMARY_KEYS_TS = "primary_keys_ts"
    """file with primary keys and timestamps as of now, before load"""
    LAST_PK_VERSION = "last_pk_version"
    """
    file with primary keys and timestamps as of last load
    """

    LATEST_PK_VERSION = "latest_pk_version"
    """file with primary keys and timestamps as of after load. 
      this will be identital to primary_keys file IF there are no updates in the source within the load. 
      this is unlikely, but possible
    """


def _all_nullable(schema: "pa.Schema") -> "pa.Schema":
    import pyarrow.types as pat

    sc = schema
    for i, n in enumerate(schema.names):
        f = schema.field(n)
        if not f.nullable:
            sc = sc.set(i, f.with_nullable(True))
    return sc


def _cast(
    name: str,
    data_type: str,
    *,
    table_alias: str | None = None,
    flavor: Literal["tsql", "duckdb"],
):
    if data_type in ["datetime", "datetime2"] and flavor == "tsql":
        return ex.cast(ex.column(name, table_alias), ex.DataType(this="datetime2(6)"))

    if data_type in ["rowversion", "timestamp"] and flavor == "tsql":
        return ex.cast(ex.column(name, table_alias), ex.DataType.Type.BIGINT)
    return ex.column(name, table_alias)


valid_from_expr = ex.cast(
    ex.func("GETUTCDATE", dialect="tsql"), ex.DataType(this="datetime2(6)")
).as_(VALID_FROM_COL_NAME)


def _get_cols_select(
    cols: list[InformationSchemaColInfo],
    *,
    is_deleted: bool | None = None,
    is_full: bool | None = None,
    with_valid_from: bool = False,
    table_alias: str | None = None,
    flavor: Literal["tsql", "duckdb"],
) -> Sequence[ex.Expression]:
    return (
        [
            _cast(
                c.column_name,
                c.data_type,
                table_alias=table_alias,
                flavor=flavor,
            ).as_(c.column_name)
            for c in cols
        ]
        + ([valid_from_expr] if with_valid_from else [])
        + (
            [ex.cast(ex.convert(int(is_deleted)), "bit").as_(IS_DELETED_COL_NAME)]
            if is_deleted is not None
            else []
        )
        + (
            [ex.cast(ex.convert(int(is_full)), "bit").as_(IS_FULL_LOAD_COL_NAME)]
            if is_full is not None
            else []
        )
    )


def get_delta_col(
    cols: list[InformationSchemaColInfo],
) -> InformationSchemaColInfo | None:
    row_start_col: InformationSchemaColInfo | None = None
    for c in cols:
        if c.data_type.lower() in ["rowversion", "timestamp"]:
            return c
        if c.generated_always_type_desc == "AS_ROW_START":
            row_start_col = c
    return row_start_col


async def write_db_to_delta(
    connection_string: str,
    table: tuple[str, str],
    folder: Path,
    conn: pyodbc.Connection | None = None,
):
    delta_path = folder / "delta"
    owns_con = False
    if conn is None:
        conn = pyodbc.connect(connection_string)
        owns_con = True
    cols = get_columns(conn, table)[table]

    os.makedirs(folder / "meta", exist_ok=True)
    with open(folder / "meta/schema.json", "w", encoding="utf-8") as f:
        json.dump([c.model_dump() for c in cols], f, indent=4)
    if (folder / "delta_load_backup").exists():
        shutil.rmtree(folder / "delta_load_backup")
    if (folder / "delta_load").exists():
        shutil.move(folder / "delta_load", folder / "delta_load_backup")
        shutil.copytree(
            folder / f"delta_load_backup/{DBDeltaPathConfigs.LATEST_PK_VERSION}",
            folder / f"delta_load/{DBDeltaPathConfigs.LAST_PK_VERSION}",
        )

    try:
        lock_file_path = folder / "meta/lock.txt"
        if (
            os.path.exists(lock_file_path)
            and (time.time() - os.path.getmtime(lock_file_path)) > 60 * 60
        ):
            os.remove(lock_file_path)
        with open(
            lock_file_path, "x", encoding="utf-8"
        ) as f:  # lock file to avoid duplicate loading
            delta_col = get_delta_col(cols)  # Use the imported function
            pks = get_primary_keys(conn, table)[table]  # Use the imported function
            if not (delta_path / "_delta_log").exists():
                os.makedirs(delta_path, exist_ok=True)
                do_full_load(
                    connection_string,
                    table,
                    delta_path,
                    mode="overwrite",
                    cols=cols,
                    pks=pks,
                    delta_col=delta_col,
                )
            else:
                if delta_col is None or len(pks) == 0:
                    do_full_load(
                        connection_string,
                        table,
                        delta_path,
                        mode="append",
                        cols=cols,
                        pks=pks,
                        delta_col=delta_col,
                    )
                else:
                    await do_delta_load(
                        connection_string,
                        table,
                        folder=folder,
                        delta_col=delta_col,
                        cols=cols,
                        pks=pks,
                        db_conn=conn,
                    )
        os.remove(lock_file_path)
    finally:
        if owns_con:
            conn.close()


def restore_last_pk(
    local_con: duckdb.DuckDBPyConnection,
    folder: Path,
    delta_table: DeltaTable,
    delta_col: InformationSchemaColInfo,
    pk_cols: list[InformationSchemaColInfo],
):
    delta_table.update_incremental()
    pks = [c.column_name for c in pk_cols]
    with local_con.cursor() as cur:
        sq_valid_from = get_sql_for_delta_expr(
            delta_table,
            select=[ex.func("max", ex.column(VALID_FROM_COL_NAME))],
            conditions=[ex.column(IS_FULL_LOAD_COL_NAME).eq(True)],
        )
        assert sq_valid_from is not None
        cur.execute(sq_valid_from.sql(dialect="duckdb"))
        max_valid_from_res = cur.fetchone()
        latest_full_load_date = max_valid_from_res[0] if max_valid_from_res else None

        last_full_load_expr = get_sql_for_delta_expr(
            delta_table,
            select=_get_cols_select(
                cols=pk_cols + [delta_col],
                table_alias="tr",
                flavor="duckdb",
            ),
            conditions=[
                ex.column(IS_FULL_LOAD_COL_NAME).eq(True),
                ex.column(VALID_FROM_COL_NAME).eq(
                    ex.subquery(
                        ex.select(ex.func("MAX", ex.column(VALID_FROM_COL_NAME)))
                        .from_("tr")
                        .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
                    )
                ),
            ],
            delta_table_cte_name="tr",
        )
        assert last_full_load_expr is not None
        cur.execute(
            "create view last_full_load as " + last_full_load_expr.sql("duckdb")
        )
    with local_con.cursor() as cur:
        sq = get_sql_for_delta_expr(
            delta_table,
            select=_get_cols_select(
                cols=pk_cols + [delta_col, IS_DELETED_COL_INFO],
                table_alias="tr",
                flavor="duckdb",
            ),
            conditions=[
                ex.column(VALID_FROM_COL_NAME) > ex.convert(latest_full_load_date)
            ],
        )
        assert sq is not None
        sq = sq.qualify(
            ex.EQ(
                this=ex.Window(
                    this=ex.RowNumber(),
                    partition_by=[ex.column(pk) for pk in pks],
                    order=ex.Order(
                        expressions=[
                            ex.Ordered(
                                this=ex.column(delta_col.column_name),
                                desc=True,
                                nulls_first=False,
                            )
                        ]
                    ),
                    over="OVER",
                ),
                expression=ex.convert(1),
            )
        )
        cur.execute("create view delta_after_full_load as " + sq.sql("duckdb"))
    with local_con.cursor() as cur:
        cur.execute(
            f"""create view v_last_pk_version as
                with base as ( 
                select df.* from delta_after_full_load df
                union all
                select f.*, (cast 0 as BOOLEAN) as {IS_DELETED_COL_NAME} from last_full_load f 
                    anti join delta_after_full_load d on {', '.join([f"f.{sql_quote_name(c.column_name)} = d.{sql_quote_name(c.column_name)}" for c in pk_cols])}
                )
                select * except({IS_DELETED_COL_NAME}) from base where {IS_DELETED_COL_NAME} = 0
                    """
        )

    with local_con.cursor() as cur:
        cur.execute("select * from v_last_pk_version")
        rdr = cur.fetch_record_batch()
        write_deltalake(
            folder / f"delta_load/{DBDeltaPathConfigs.LAST_PK_VERSION}",
            rdr,
            mode="overwrite",
            schema_mode="overwrite",
            writer_properties=WriterProperties(compression="ZSTD"),
            engine="rust",
        )


def create_replace_view(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    format: Literal["parquet", "delta"],
    base_folder: Path,
):
    with conn.cursor() as cur:
        if format == "parquet":
            location = base_folder / f"delta_load/{name}.parquet"
            cur.execute(
                f"create or replace view {name} as select * from read_parquet('{str(location)}')"
            )
        else:
            location = base_folder / f"delta_load/{name}"
            cur.execute(
                f"create or replace view {name} as "
                + _not_none(get_sql_for_delta_expr(location)).sql("duckdb")
            )


def write_latest_pk(
    folder: Path,
    pks: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo,
):
    with duckdb.connect() as local_con:
        delta_1_path = folder / f"delta_load/{DBDeltaPathConfigs.DELTA_1_NAME}.parquet"
        delta_2_path = (
            folder / f"delta_load/{DBDeltaPathConfigs.DELTA_2_NAME}.parquet"
        )  # we don't always have this one
        current_pk_path = (
            folder / f"delta_load/{DBDeltaPathConfigs.PRIMARY_KEYS_TS}.parquet"
        )

        with local_con.cursor() as cur:
            select_expr = ex.select(
                *_get_cols_select(
                    cols=pks + [delta_col],
                    table_alias="t",
                    flavor="duckdb",
                )
            )
            if os.path.exists(delta_2_path):
                create_replace_view(
                    local_con, DBDeltaPathConfigs.DELTA_2_NAME, "parquet", folder
                )
            else:
                cur.execute(
                    "create view delta_2 as "
                    + select_expr.from_(read_parquet(delta_1_path).as_("t"))
                    .where("1=0")
                    .sql("duckdb")
                )

        with local_con.cursor() as cur:
            latest_pk_query = union(
                [
                    ex.select(
                        *_get_cols_select(
                            cols=pks + [delta_col],
                            table_alias="au",
                            flavor="duckdb",
                        )
                    ).from_(table_from_tuple("delta_2", alias="au")),
                    (
                        ex.select(
                            *_get_cols_select(
                                cols=pks + [delta_col],
                                table_alias="d1",
                                flavor="duckdb",
                            )
                        )
                        .from_(read_parquet(delta_1_path).as_("d1"))
                        .join(
                            ex.table_("delta_2", alias="au2"),
                            ex.and_(
                                *[
                                    ex.column(c.column_name, "d1").eq(
                                        ex.column(c.column_name, "au2")
                                    )
                                    for c in pks
                                ]
                            ),
                            join_type="anti",
                        )
                    ),
                    (
                        ex.select(
                            *_get_cols_select(
                                cols=pks + [delta_col],
                                table_alias="cpk",
                                flavor="duckdb",
                            )
                        )
                        .from_(read_parquet(current_pk_path).as_("cpk"))
                        .join(
                            ex.table_("delta_2", alias="au3"),
                            ex.and_(
                                *[
                                    ex.column(c.column_name, "cpk").eq(
                                        ex.column(c.column_name, "au3")
                                    )
                                    for c in pks
                                ]
                            ),
                            join_type="anti",
                        )
                        .join(
                            read_parquet(delta_1_path).as_("au4"),
                            ex.and_(
                                *[
                                    ex.column(c.column_name, "cpk").eq(
                                        ex.column(c.column_name, "au4")
                                    )
                                    for c in pks
                                ]
                            ),
                            join_type="anti",
                        )
                    ),
                ],
                distinct=False,
            )
            cur.execute(latest_pk_query.sql("duckdb"))
            write_deltalake(
                folder / f"delta_load/{DBDeltaPathConfigs.LATEST_PK_VERSION}",
                cur.fetch_record_batch(),
                mode="overwrite",
                schema_mode="overwrite",
                writer_properties=WriterProperties(compression="ZSTD"),
                engine="rust",
            )


async def do_delta_load(
    connection_string: str,
    table: table_name_type,
    folder: Path,
    *,
    delta_col: InformationSchemaColInfo,
    cols: list[InformationSchemaColInfo],
    pks: list[str],
    db_conn: pyodbc.Connection,
):
    last_pk_path = folder / f"delta_load/{DBDeltaPathConfigs.LAST_PK_VERSION}"

    with duckdb.connect() as local_con:
        if not (last_pk_path / "_delta_log").exists():  # or do a full load?
            restore_last_pk(
                local_con,
                folder,
                DeltaTable(folder / "delta"),
                delta_col,
                [c for c in cols if c.column_name in pks],
            )
        delta_path = folder / "delta"
        dt = DeltaTable(delta_path)
        sq = get_sql_for_delta_expr(
            dt,
            select=[
                ex.func(
                    "MAX",
                    _cast(
                        delta_col.column_name,
                        delta_col.data_type,
                        flavor="duckdb",
                    ),
                )
            ],
        )
        assert sq is not None
        sql = sq.sql("duckdb")
        with local_con.cursor() as cur:
            cur.execute(sql)
            res = cur.fetchone()
            delta_load_value = res[0] if res else None
        print(delta_load_value)
        if delta_load_value is None:
            do_full_load(
                connection_string,
                table,
                delta_path,
                mode="append",
                cols=cols,
                pks=pks,
                delta_col=delta_col,
            )
            return
        pk_cols = [c for c in cols if c.column_name in pks]
        pk_ds_cols = pk_cols + [delta_col]
        assert len(pk_ds_cols) == len(pks) + 1
        _retrieve_primary_key_data(
            connection_string=connection_string,
            table=table,
            delta_col=delta_col,
            pk_cols=pk_cols,
            folder=folder,
        )

        criterion = _cast(
            delta_col.column_name, delta_col.data_type, table_alias="t", flavor="tsql"
        ) > ex.convert(delta_load_value)
        _load_updates_to_delta(
            connection_string,
            table,
            delta_path,
            cols=cols,
            local_con=local_con,
            criterion=criterion,
            delta_name="delta_1",
        )
        dt.update_incremental()

        await _handle_additional_updates(
            connection_string=connection_string,
            table=table,
            delta_path=delta_path,
            db_conn=db_conn,
            local_con=local_con,
            pk_cols=pk_cols,
            delta_col=delta_col,
            cols=cols,
        )
        dt.update_incremental()
        write_latest_pk(folder, pk_cols, delta_col)

        do_deletes(
            folder=folder,
            local_con=local_con,
            delta_table=dt,
            cols=cols,
            pk_cols=pk_cols,
        )


def do_deletes(
    folder: Path,
    local_con: duckdb.DuckDBPyConnection,
    delta_table: DeltaTable,
    cols: list[InformationSchemaColInfo],
    pk_cols: list[InformationSchemaColInfo],
):
    create_replace_view(
        local_con, DBDeltaPathConfigs.LATEST_PK_VERSION, "delta", folder
    )
    create_replace_view(local_con, DBDeltaPathConfigs.LAST_PK_VERSION, "delta", folder)
    delete_query = ex.except_(
        left=ex.select(
            *_get_cols_select(pk_cols, table_alias="lpk", flavor="duckdb")
        ).from_(table_from_tuple(DBDeltaPathConfigs.LAST_PK_VERSION, alias="lpk")),
        right=ex.select(
            *_get_cols_select(pk_cols, table_alias="cpk", flavor="duckdb")
        ).from_(table_from_tuple(DBDeltaPathConfigs.LATEST_PK_VERSION, alias="cpk")),
    )

    with local_con.cursor() as cur:
        cur.execute("create view deletes as " + delete_query.sql("duckdb"))
    with local_con.cursor() as cur:
        non_pk_cols = [c for c in cols if c not in pk_cols]
        non_pk_select = [ex.Null().as_(c.column_name) for c in non_pk_cols]
        deletes_with_schema = union(
            [
                ex.select(
                    *_get_cols_select(
                        pk_cols,
                        table_alias="d1",
                        flavor="duckdb",
                    )
                )
                .select(
                    *_get_cols_select(
                        non_pk_cols,
                        table_alias="d1",
                        flavor="duckdb",
                    ),
                    append=True,
                )
                .select(
                    ex.AtTimeZone(
                        this=ex.CurrentTimestamp(),
                        zone=ex.Literal(this="UTC", is_string=True),
                    ).as_(VALID_FROM_COL_NAME),
                    ex.convert(True).as_(IS_DELETED_COL_NAME),
                    ex.convert(False).as_(IS_FULL_LOAD_COL_NAME),
                )
                .from_(table_from_tuple("delta_1", alias="d1"))
                .where("1=0"),  # only used to get correct datatypes
                ex.select(
                    ex.Column(
                        this=ex.Star(), table=ex.Identifier(this="d", quoted=False)
                    )
                )
                .select(*non_pk_select, append=True)
                .select(
                    ex.AtTimeZone(
                        this=ex.CurrentTimestamp(),
                        zone=ex.Literal(this="UTC", is_string=True),
                    ).as_(VALID_FROM_COL_NAME),
                    append=True,
                )
                .select(ex.convert(True).as_(IS_DELETED_COL_NAME), append=True)
                .select(ex.convert(False).as_(IS_FULL_LOAD_COL_NAME), append=True)
                .from_(table_from_tuple("deletes", alias="d")),
            ],
            distinct=False,
        )

        cur.execute(
            "create view deletes_with_schema as " + deletes_with_schema.sql("duckdb")
        )
    with local_con.cursor() as cur:
        cur.execute(
            "select count(*) from (select * from deletes_with_schema limit 1) s"
        )
        res = cur.fetchone()
        assert res is not None
        res = res[0]
        has_deletes = res > 0
    if has_deletes:
        with local_con.cursor() as cur:
            cur.execute(deletes_with_schema.sql("duckdb"))
            rdr = cur.fetch_record_batch()
            write_deltalake(
                delta_table,
                rdr,
                schema=_all_nullable(rdr.schema),
                mode="append",
                schema_mode="merge",
                writer_properties=WriterProperties(compression="ZSTD"),
                engine="rust",
            )


def _retrieve_primary_key_data(
    connection_string: str,
    table: table_name_type,
    delta_col: InformationSchemaColInfo,
    pk_cols: list[InformationSchemaColInfo],
    folder: Path,
):
    pk_ts_col_select = ex.select(
        *_get_cols_select(
            is_full=None,
            is_deleted=None,
            cols=pk_cols + [delta_col],
            with_valid_from=False,
            flavor="tsql",
        )
    ).from_(table_from_tuple(table))
    pk_ts_reader_sql = pk_ts_col_select.sql("tsql")

    pk_path = folder / f"delta_load/{DBDeltaPathConfigs.PRIMARY_KEYS_TS}.parquet"

    write_sql_to_parquet(
        sql=pk_ts_reader_sql, connection_string=connection_string, parquet_path=pk_path
    )
    return pk_path

    # todo: test
    # todo: deletes
    # todo: persist latest ts per pk?


async def _handle_additional_updates(
    connection_string: str,
    table: table_name_type,
    delta_path: Path,
    db_conn: pyodbc.Connection,
    local_con: duckdb.DuckDBPyConnection,
    pk_cols: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo,
    cols: list[InformationSchemaColInfo],
):
    """Handles updates that are not logical by their timestamp. This can happen on a restore from backup, for example."""
    folder = delta_path.parent
    pk_ds_cols = pk_cols + [delta_col]
    create_replace_view(
        local_con, DBDeltaPathConfigs.PRIMARY_KEYS_TS, "parquet", folder
    )
    create_replace_view(local_con, DBDeltaPathConfigs.LAST_PK_VERSION, "delta", folder)
    with local_con.cursor() as cur:
        cur.execute(
            ex.except_(
                left=ex.select(
                    *_get_cols_select(
                        cols=pk_ds_cols,
                        table_alias="pk",
                        flavor="duckdb",
                    )
                ).from_(
                    table_from_tuple(DBDeltaPathConfigs.PRIMARY_KEYS_TS, alias="pk")
                ),
                right=ex.select(
                    *_get_cols_select(
                        cols=pk_ds_cols,
                        table_alias="lpk",
                        flavor="duckdb",
                    )
                ).from_(
                    table_from_tuple(DBDeltaPathConfigs.LAST_PK_VERSION, alias="lpk")
                ),
            ).sql("duckdb")
        )
        rdr = cur.fetch_record_batch()
        additional_updates_path = folder / "delta_load/additional_updates.parquet"
        write_to_parquet(additional_updates_path, rdr)
    with local_con.cursor() as cur:
        sql_query = ex.except_(
            left=ex.select(
                *_get_cols_select(
                    cols=pk_cols,
                    table_alias="au",
                    flavor="duckdb",
                )
            ).from_(read_parquet(additional_updates_path).as_("au")),
            right=ex.select(
                *_get_cols_select(
                    cols=pk_cols,
                    table_alias="d1",
                    flavor="duckdb",
                )
            ).from_(table_from_tuple("delta_1", alias="d1")),
        )
        cur.execute(
            """create or replace view real_additional_updates as """
            + sql_query.sql("duckdb")
        )
    with local_con.cursor() as cur:
        cur.execute(
            "select count(*) from (select * from real_additional_updates limit 1) s"
        )
        res = cur.fetchone()
        assert res is not None
        res = res[0]
        has_additional_updates = res > 0
    if has_additional_updates:
        temp_table_name = "##temp_updates_" + str(hash(table[1]))[1:]
        sql = get_sql_for_schema(
            temp_table_name,
            [p.as_field_type() for p in pk_cols],
            primary_keys=None,
            with_exist_check=False,
        )
        with db_conn.cursor() as cur:
            cur.execute(sql)
        from .odbc_insert import pyodbc_insert_into_table

        await pyodbc_insert_into_table(
            reader=local_con.execute(
                "select * from real_additional_updates"
            ).fetch_record_batch(),
            table_name=temp_table_name,
            connection=db_conn,
            schema=[p.as_field_type() for p in pk_cols],
        )
        criterion = f"""
            inner join {temp_table_name} ttt on {', '.join([f't.{sql_quote_name(c.column_name)} = ttt.{sql_quote_name(c.column_name)}' for c in pk_cols])}"""

        _load_updates_to_delta(
            connection_string=connection_string,
            table=table,
            delta_path=delta_path,
            cols=cols,
            local_con=local_con,
            criterion=criterion,
            delta_name="delta_2",
        )
    else:
        if os.path.exists(folder / "delta_load/delta_2.parquet"):
            os.remove(folder / "delta_load/delta_2.parquet")


def _load_updates_to_delta(
    connection_string: str,
    table: table_name_type,
    delta_path: Path,
    cols: list[InformationSchemaColInfo],
    local_con: duckdb.DuckDBPyConnection,
    criterion: str | Sequence[str | ex.Expression] | ex.Expression,
    delta_name: str,
):
    if isinstance(criterion, ex.Expression):
        criterion = [criterion]
    delta_sql = (
        ex.select(
            *_get_cols_select(
                cols,
                is_full=False,
                is_deleted=False,
                with_valid_from=True,
                table_alias="t",
                flavor="tsql",
            )
        )
        .where(*(criterion if not isinstance(criterion, str) else []), dialect="tsql")
        .from_(table_from_tuple(table, alias="t"))
        .sql("tsql")
    )
    if isinstance(criterion, str):
        delta_sql += " " + criterion

    delta_name_path = delta_path.parent / f"delta_load/{delta_name}.parquet"
    write_sql_to_parquet(
        sql=delta_sql, connection_string=connection_string, parquet_path=delta_name_path
    )
    with local_con.cursor() as cur:
        cur.execute(
            f'create view {delta_name} as select * from read_parquet("{str(delta_name_path)}")'
        )

    with local_con.cursor() as cur:
        cur.execute(f"select count(*) from (select * from {delta_name} limit 1)")
        res = cur.fetchone()
        assert res is not None
        res = res[0]
        if res == 0:
            return
    with local_con.cursor() as cur:
        cur.execute(f"select * from {delta_name}")
        rdr = cur.fetch_record_batch()
        write_deltalake(
            delta_path,
            rdr,
            schema=_all_nullable(rdr.schema),
            mode="append",
            writer_properties=WriterProperties(compression="ZSTD"),
            engine="rust",
            schema_mode="merge",
        )


def write_sql_to_parquet(connection_string: str, sql: str, parquet_path: str | Path):
    batch_reader = read_arrow_batches_from_odbc(
        query=sql,
        connection_string=connection_string,
        max_binary_size=20000,
        max_text_size=20000,
    )
    with pq.ParquetWriter(parquet_path, batch_reader.schema) as writer:
        for batch in batch_reader:
            writer.write_batch(batch)


def write_to_parquet(parquet_path: str | Path, batch_reader: pa.RecordBatchReader):
    parquet_path = Path(parquet_path) if isinstance(parquet_path, str) else parquet_path
    temp_path = parquet_path.with_suffix(".tmp.parquet")
    if temp_path.exists():
        os.remove(temp_path)
    with pq.ParquetWriter(temp_path, batch_reader.schema) as writer:
        for batch in batch_reader:
            writer.write_batch(batch)
    os.replace(temp_path, parquet_path)


def do_full_load(
    connection_string: str,
    table: table_name_type,
    delta_path: Path,
    mode: Literal["overwrite", "append"],
    cols: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo | None,
    pks: list[str],
):
    sql = (
        ex.select(
            *_get_cols_select(
                is_deleted=False,
                is_full=True,
                cols=cols,
                with_valid_from=True,
                flavor="tsql",
            )
        )
        .from_(table_from_tuple(table))
        .sql("tsql")
    )
    reader = read_arrow_batches_from_odbc(
        query=sql,
        connection_string=connection_string,
        max_binary_size=20000,
        max_text_size=20000,
    )
    try:
        dt = DeltaTable(delta_path)
        old_add_actions = dt.get_add_actions().to_pylist()
        old_paths = [ac["path"] for ac in old_add_actions]
    except TableNotFoundError:
        old_paths = []
        dt = None

    write_deltalake(
        delta_path,
        reader,
        schema=_all_nullable(reader.schema),
        mode=mode,
        writer_properties=WriterProperties(compression="ZSTD"),
        schema_mode="overwrite" if mode == "overwrite" else "merge",
        engine="rust",
    )
    if delta_col is None:
        return
    dt = dt or DeltaTable(delta_path)
    dt.update_incremental()
    delta_path.parent.joinpath("delta_load").mkdir(exist_ok=True)
    with duckdb.connect() as local_con:
        sql = get_sql_for_delta_expr(
            dt,
            select=[pk for pk in pks] + ([delta_col.column_name] if delta_col else []),
            action_filter=lambda ac: ac["path"] not in old_paths,
        )
        assert sql is not None
        with local_con.cursor() as cur:
            cur.execute(sql.sql("duckdb"))
            write_deltalake(
                delta_path.parent
                / f"delta_load/{DBDeltaPathConfigs.LATEST_PK_VERSION}",
                cur.fetch_record_batch(),
                mode="overwrite",
                schema_mode="overwrite",
                writer_properties=WriterProperties(compression="ZSTD"),
                engine="rust",
            )
