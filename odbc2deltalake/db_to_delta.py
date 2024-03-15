from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Literal, Sequence, TypeVar
from arrow_odbc import read_arrow_batches_from_odbc
from deltalake2db.duckdb import apply_storage_options
import asyncio
import sqlglot as sg
from odbc2deltalake.destination import (
    AzureDestination,
    Destination,
    FileSystemDestination,
)
from odbc2deltalake.reader import DataSourceReader
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
from .odbc_utils import build_connection_string
import shutil
import logging

logger = logging.getLogger(__name__)

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
    source: DataSourceReader | str,
    table: tuple[str, str],
    destination: Destination | Path,
):
    if isinstance(destination, Path):
        destination = FileSystemDestination(destination)
    if isinstance(source, str):
        from .reader import ODBCReader

        source = ODBCReader(source)
    delta_path = destination / "delta"
    owns_con = False
    cols = get_columns(source, table)

    (destination / "meta").mkdir()
    (destination / "meta/schema.json").upload(
        json.dumps([c.model_dump() for c in cols], indent=4).encode("utf-8")
    )
    if (destination / "delta_load_backup").exists():
        (destination / "delta_load_backup").rm_tree()
    if (destination / "delta_load").exists():
        fs, path = destination.get_fs_path()
        (destination / "delta_load").path_rename(destination / "delta_load_backup")  # type: ignore

        if (
            destination / "delta_load_backup" / DBDeltaPathConfigs.LATEST_PK_VERSION
        ).exists():
            (destination / "delta_load_backup" / DBDeltaPathConfigs.LATEST_PK_VERSION).path_copy(destination / "delta_load" / DBDeltaPathConfigs.LAST_PK_VERSION)  # type: ignore

    lock_file_path = destination / "meta/lock.txt"

    try:
        if (
            lock_file_path.exists()
            and (
                datetime.now(tz=timezone.utc) - lock_file_path.modified_time()
            ).total_seconds()
            > 60 * 60
        ):
            lock_file_path.remove()
        lock_file_path.upload(b"")

        delta_col = get_delta_col(cols)  # Use the imported function
        pks = get_primary_keys(source, table)  # Use the imported function

        if not (delta_path / "_delta_log").exists():
            delta_path.mkdir()
            do_full_load(
                source,
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
                    source,
                    table,
                    delta_path,
                    mode="append",
                    cols=cols,
                    pks=pks,
                    delta_col=delta_col,
                )
            else:
                await do_delta_load(
                    source,
                    table,
                    destination=destination,
                    delta_col=delta_col,
                    cols=cols,
                    pks=pks,
                )
        lock_file_path.remove()
    except Exception as e:
        # restore files
        if lock_file_path.exists():
            lock_file_path.remove()
        if (destination / "delta_load").exists():
            (destination / "delta_load").rm_tree()
        fs, path = destination.get_fs_path()
        if (destination / "delta_load_backup").exists():
            fs.move(
                path + "/" + "delta_load_backup",
                path + "/" + "delta_load",
                recursive=True,
            )
        raise e


def restore_last_pk(
    reader: DataSourceReader,
    table: table_name_type,
    destination: Destination,
    delta_col: InformationSchemaColInfo,
    pk_cols: list[InformationSchemaColInfo],
):
    delta_path = destination / "delta"
    reader.local_register_update_view(delta_path, _temp_table(table))

    pks = [c.column_name for c in pk_cols]

    sq_valid_from = reader.local_execute_sql_to_py(
        sg.from_(_temp_table(table))
        .select(ex.func("max", ex.column(VALID_FROM_COL_NAME)).as_(VALID_FROM_COL_NAME))
        .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
    )
    if sq_valid_from is None or len(sq_valid_from) == 0:
        return False
    latest_full_load_date = sq_valid_from[0][VALID_FROM_COL_NAME]
    reader.local_register_view(
        sg.from_(ex.table_(_temp_table(table), alias="tr"))
        .select(
            *_get_cols_select(
                cols=pk_cols + [delta_col],
                table_alias="tr",
                flavor="duckdb",
            )
        )
        .where(
            ex.column(IS_FULL_LOAD_COL_NAME).eq(True)
            and ex.column(VALID_FROM_COL_NAME).eq(
                ex.Subquery(
                    this=ex.select(ex.func("MAX", ex.column(VALID_FROM_COL_NAME)))
                    .from_("tr")
                    .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
                )
            )
        ),
        "last_full_load",
    )

    sq = (
        sg.from_(ex.table_(_temp_table(table), alias="tr"))
        .select(
            *_get_cols_select(
                cols=pk_cols + [delta_col, IS_DELETED_COL_INFO],
                table_alias="tr",
                flavor="duckdb",
            )
        )
        .where(ex.column(VALID_FROM_COL_NAME) > ex.convert(latest_full_load_date))
    )
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
    reader.local_register_view(sq, "delta_after_full_load")
    reader.local_register_view(
        sq.from_("base")
        .where(ex.column(IS_DELETED_COL_NAME))
        .with_(
            "base",
            as_=ex.union(
                left=sq.from_("delta_after_full_load").select("*"),
                right=sq.from_(ex.table_("last_full_load", "f")).join(
                    ex.table_("delta_after_full_load", "d"),
                    join_type="anti",
                    on=ex.and_(
                        *[
                            ex.column(c.column_name, "f").eq(
                                ex.column(c.column_name, "d")
                            )
                            for c in pk_cols
                        ]
                    ),
                ),
                distinct=False,
            ),
        )
        .select(ex.Star(**{"except": [ex.column(IS_DELETED_COL_NAME)]})),
        "v_last_pk_version",
    )
    cnt = reader.local_execute_sql_to_py(
        "select count(*) as cnt from v_last_pk_version"
    )[0]["cnt"]
    if cnt == 0:
        return False
    reader.local_execute_sql_to_delta(
        sq.from_("v_last_pk_version").select(ex.Star()),
        destination / "delta_load" / DBDeltaPathConfigs.LAST_PK_VERSION,
        mode="overwrite",
    )
    return True


def create_replace_view(
    reader: DataSourceReader,
    name: str,
    base_destination: Destination,
):
    reader.local_register_update_view(base_destination / f"delta_load/{name}", name)


def write_latest_pk(
    reader: DataSourceReader,
    destination: Destination,
    pks: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo,
):
    reader.local_register_update_view(
        destination / f"delta_load/{DBDeltaPathConfigs.DELTA_1_NAME}",
        DBDeltaPathConfigs.DELTA_1_NAME,
    )

    reader.local_register_update_view(
        destination / f"delta_load/{DBDeltaPathConfigs.DELTA_2_NAME}",
        DBDeltaPathConfigs.DELTA_2_NAME,
    )
    reader.local_register_update_view(
        destination / f"delta_load/{DBDeltaPathConfigs.PRIMARY_KEYS_TS}",
        DBDeltaPathConfigs.PRIMARY_KEYS_TS,
    )

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
                .from_(ex.table_(DBDeltaPathConfigs.DELTA_1_NAME, alias="d1"))
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
                .from_(ex.table_(DBDeltaPathConfigs.PRIMARY_KEYS_TS, alias="cpt"))
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
                    ex.table_(DBDeltaPathConfigs.DELTA_1_NAME, alias="au4"),
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
    reader.local_execute_sql_to_delta(
        latest_pk_query,
        destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        mode="overwrite",
    )


async def do_delta_load(
    reader: DataSourceReader,
    table: table_name_type,
    destination: Destination,
    *,
    delta_col: InformationSchemaColInfo,
    cols: list[InformationSchemaColInfo],
    pks: list[str],
):
    last_pk_path = destination / f"delta_load/{DBDeltaPathConfigs.LAST_PK_VERSION}"
    logger.info(
        f"{table}: Start Delta Load with Delta Column {delta_col.column_name} and pks: {', '.join(pks)}"
    )

    with duckdb.connect() as local_con:
        fs_opts = destination.as_path_options("fsspec")[1]
        if fs_opts:
            apply_storage_options(local_con, fs_opts)

        if not (last_pk_path / "_delta_log").exists():  # or do a full load?
            logger.warning(f"{table}: Primary keys missing, try to restore")
            if not restore_last_pk(
                table,
                local_con,
                destination,
                (destination / "delta").as_delta_table(),
                delta_col,
                [c for c in cols if c.column_name in pks],
            ):
                logger.warning(f"{table}: No primary keys found, do a full load")
                do_full_load(
                    reader,
                    table,
                    destination / "delta",
                    mode="append",
                    cols=cols,
                    pks=pks,
                    delta_col=delta_col,
                )
                return
        delta_path = destination / "delta"
        dt = delta_path.as_delta_table()
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
        if delta_load_value is None:
            logger.warning(f"{table}: No delta load value, do a full load")
            do_full_load(
                reader,
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
        logger.info(
            f"{table}: Start delta step 1, get primary keys and timestamps. MAX({delta_col.column_name}): {delta_load_value}"
        )
        _retrieve_primary_key_data(
            reader=reader,
            table=table,
            delta_col=delta_col,
            pk_cols=pk_cols,
            destination=destination,
        )

        criterion = _cast(
            delta_col.column_name, delta_col.data_type, table_alias="t", flavor="tsql"
        ) > ex.convert(delta_load_value)
        logger.info(f"{table}: Start delta step 2, load updates by timestamp")
        _load_updates_to_delta(
            reader,
            table,
            delta_path,
            cols=cols,
            local_con=local_con,
            criterion=criterion,
            delta_name="delta_1",
        )
        dt.update_incremental()

        await _handle_additional_updates(
            reader=reader,
            table=table,
            delta_path=delta_path,
            db_conn=db_conn,
            local_con=local_con,
            pk_cols=pk_cols,
            delta_col=delta_col,
            cols=cols,
        )
        dt.update_incremental()

        logger.info(f"{table}: Start delta step 3.5, write meta for next delta load")

        write_latest_pk(destination, pk_cols, delta_col)

        logger.info(f"{table}: Start delta step 4.5, write deletes")
        do_deletes(
            destination=destination,
            local_con=local_con,
            delta_table=dt,
            cols=cols,
            pk_cols=pk_cols,
        )
        logger.info(f"{table}: Done delta load")


def do_deletes(
    destination: Destination,
    local_con: duckdb.DuckDBPyConnection,
    # delta_table: DeltaTable,
    cols: list[InformationSchemaColInfo],
    pk_cols: list[InformationSchemaColInfo],
):
    create_replace_view(
        local_con, DBDeltaPathConfigs.LATEST_PK_VERSION, "delta", destination
    )
    create_replace_view(
        local_con, DBDeltaPathConfigs.LAST_PK_VERSION, "delta", destination
    )
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
                writer_properties=WRITER_PROPERTIES,
                engine="rust",
            )


def _retrieve_primary_key_data(
    reader: DataSourceReader,
    table: table_name_type,
    delta_col: InformationSchemaColInfo,
    pk_cols: list[InformationSchemaColInfo],
    destination: Destination,
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

    pk_path = destination / f"delta_load/{DBDeltaPathConfigs.PRIMARY_KEYS_TS}.parquet"

    reader.write_sql_to_parquet(sql=pk_ts_reader_sql, parquet_path=pk_path)
    return pk_path

    # todo: test
    # todo: deletes
    # todo: persist latest ts per pk?


async def _handle_additional_updates(
    reader: DataSourceReader,
    table: table_name_type,
    delta_path: Destination,
    pk_cols: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo,
    cols: list[InformationSchemaColInfo],
):
    """Handles updates that are not logical by their timestamp. This can happen on a restore from backup, for example."""
    folder = delta_path.parent
    pk_ds_cols = pk_cols + [delta_col]
    create_replace_view(reader, DBDeltaPathConfigs.PRIMARY_KEYS_TS, folder)
    create_replace_view(reader, DBDeltaPathConfigs.LAST_PK_VERSION, folder)
    reader.local_register_view(
        ex.except_(
            left=ex.select(
                *_get_cols_select(
                    cols=pk_ds_cols,
                    table_alias="pk",
                    flavor="duckdb",
                )
            ).from_(ex.table_(DBDeltaPathConfigs.PRIMARY_KEYS_TS, alias="pk")),
            right=ex.select(
                *_get_cols_select(
                    cols=pk_ds_cols,
                    table_alias="lpk",
                    flavor="duckdb",
                )
            ).from_(table_from_tuple(DBDeltaPathConfigs.LAST_PK_VERSION, alias="lpk")),
        ),
        "additional_updates",
    )

    sql_query = ex.except_(
        left=ex.select(
            *_get_cols_select(
                cols=pk_cols,
                table_alias="au",
                flavor="duckdb",
            )
        ).from_(ex.table_("additional_updates", alias="au")),
        right=ex.select(
            *_get_cols_select(
                cols=pk_cols,
                table_alias="d1",
                flavor="duckdb",
            )
        ).from_(table_from_tuple("delta_1", alias="d1")),
    )
    reader.local_register_view(sql_query, "real_additional_updates")
    cnt = reader.local_execute_sql_to_py(
        sg.parse_one(
            "select count(*) as cnt from (select * from real_additional_updates limit 1) s",
            dialect="spark",
        )
    )[0]
    has_additional_updates = cnt > 0

    if has_additional_updates:
        logger.warning(f"{table}: Start delta step 3, load strange updates")
        temp_table_name = "##temp_updates_" + str(hash(table[1]))[1:]
        sql = get_sql_for_schema(
            temp_table_name,
            [p.as_field_type() for p in pk_cols],
            primary_keys=None,
            with_exist_check=False,
        )
        reader.source_sql_to_py(sql)
        from .odbc_insert import pyodbc_insert_into_table

        await pyodbc_insert_into_table(
            reader=local_con.execute(
                "select * from real_additional_updates"
            ).fetch_record_batch(),
            table_name=temp_table_name,
            connection=db_conn,
            schema=[p.as_field_type() for p in pk_cols],
        )

        def _collate(c: InformationSchemaColInfo):
            if c.data_type.lower() in [
                "char",
                "varchar",
                "nchar",
                "nvarchar",
                "text",
                "ntext",
            ]:
                return "COLLATE DATABASE_DEFAULT"
            return ""

        criterion = f"""
            inner join {temp_table_name} ttt on {' AND '.join([f't.{sql_quote_name(c.column_name)} {_collate(c)} = ttt.{sql_quote_name(c.column_name)}' for c in pk_cols])}"""

        _load_updates_to_delta(
            reader=reader,
            table=table,
            delta_path=delta_path,
            cols=cols,
            local_con=local_con,
            criterion=criterion,
            delta_name="delta_2",
        )
    else:
        if (folder / "delta_load/delta_2.parquet").exists():
            (folder / "delta_load/delta_2.parquet").remove()


def _load_updates_to_delta(
    reader: DataSourceReader,
    table: table_name_type,
    delta_path: Destination,
    cols: list[InformationSchemaColInfo],
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

    delta_name_path = delta_path.parent / f"delta_load/{delta_name}"
    reader.source_write_sql_to_delta(delta_sql, delta_name_path, mode="overwrite")
    reader.local_register_update_view(delta_name_path, delta_name)
    count = reader.local_execute_sql_to_py(
        sg.from_(sg.from_(delta_name).select(ex.Star()).limit(1).subquery()).select(
            ex.Count(this=ex.Star()).as_("cnt")
        )
    )[0]["cnt"]
    if count == 0:
        return
    reader.local_execute_sql_to_delta(
        sg.from_(delta_name).select(ex.Star()), delta_path, mode="append"
    )


def write_batch_to_parquet(
    parquet_path: Destination, batch_reader: pa.RecordBatchReader
):
    temp_path = parquet_path.with_suffix(".tmp.parquet")
    if temp_path.exists():
        temp_path.remove()
    fs, path = temp_path.get_fs_path()
    with pq.ParquetWriter(path, filesystem=fs, schema=batch_reader.schema) as writer:
        for batch in batch_reader:
            writer.write_batch(batch)
    if parquet_path.exists():
        parquet_path.remove()
    fs.move(path, str(parquet_path))


def _temp_table(table: table_name_type):
    if isinstance(table, str):
        return "temp_" + table
    return "temp_" + "_".join(table)


def do_full_load(
    reader: DataSourceReader,
    table: table_name_type,
    delta_path: Destination,
    mode: Literal["overwrite", "append"],
    cols: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo | None,
    pks: list[str],
):
    logger.info(f"{table}: Start Full Load")
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
    if (delta_path / "_delta_log").exists():
        reader.register_update_view(delta_path, _temp_table(table))
        res = reader.execute_local_sql_to_py(
            sg.from_(_temp_table(table)).select(
                ex.func("max", ex.column(VALID_FROM_COL_NAME)).as_(VALID_FROM_COL_NAME)
            )
        )
        max_valid_from = res[0][VALID_FROM_COL_NAME] if res else None

    reader.write_sql_to_delta(sql, delta_path, mode=mode)
    if delta_col is None:
        logger.info(f"{table}: Full Load done")
        return
    logger.info(f"{table}: Full Load done, write meta for delta load")

    reader.register_update_view(delta_path, _temp_table(table))
    (delta_path.parent / "delta_load").mkdir()
    query = sg.from_(_temp_table(table)).select(
        *(
            [ex.column(pk) for pk in pks]
            + ([ex.column(delta_col.column_name)] if delta_col else [])
        )
    )
    if max_valid_from:
        query = query.where(ex.column(VALID_FROM_COL_NAME) > ex.convert(max_valid_from))
    reader.local_execute_sql_to_delta(
        query, delta_path.parent / "delta_load" / DBDeltaPathConfigs.LAST_PK_VERSION
    )
