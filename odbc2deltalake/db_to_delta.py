from dataclasses import dataclass
import dataclasses
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Literal, Mapping, Sequence, TypeVar, cast
import asyncio
from pydantic import BaseModel
import sqlglot as sg
from odbc2deltalake.destination.destination import (
    Destination,
)
from odbc2deltalake.reader import DataSourceReader
from .query import sql_quote_name
from .metadata import (
    get_primary_keys,
    get_columns,
    table_name_type,
    InformationSchemaColInfo,
)
import json
import time
import sqlglot.expressions as ex
from .sql_glot_utils import table_from_tuple, union, count_limit_one
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

_default_type_map = {
    "datetime": ex.DataType(this="datetime2(6)"),
    "datetime2": ex.DataType(this="datetime2(6)"),
    "rowversion": ex.DataType.Type.BIGINT,
    "timestamp": ex.DataType.Type.BIGINT,
}
DEFAULT_DATA_TYPE_MAP: Mapping[str, ex.DATA_TYPE] = _default_type_map


@dataclass(frozen=True)
class WriteConfig:

    dialect: str = "tsql"
    """The sqlglot dialect to use for the SQL generation against the source"""

    primary_keys: list[str] | None = None
    """A list of primary keys to use for the delta load. If None, the primary keys will be determined from the source"""

    delta_col: str | None = None
    """The column to use for the delta load. If None, the column will be determined from the source. Should be mostly increasing to make load efficient"""

    load_mode: Literal["overwrite", "append", "force_full"] = "append"
    """The load mode to use. Attention: overwrite will not help you build scd2, the history is in the delta table only"""

    data_type_map: Mapping[str, ex.DATA_TYPE] = dataclasses.field(
        default_factory=lambda: _default_type_map.copy()
    )
    """Set this if you want to map stuff like decimal to double before writing to delta. We recommend doing so later in ETL usually"""


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
    type_map: Mapping[str, ex.DATA_TYPE] | None = None,
):
    mapped_type = type_map.get(data_type) if type_map else None
    if mapped_type:
        return ex.cast(ex.column(name, table_alias), mapped_type)
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
    source_uses_compat: bool,
    data_type_map: Mapping[str, ex.DATA_TYPE] | None = None,
) -> Sequence[ex.Expression]:
    return (
        [
            _cast(
                c.compat_name if source_uses_compat else c.column_name,
                c.data_type,
                table_alias=table_alias,
                type_map=data_type_map,
            ).as_(c.compat_name)
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


def _vacuum(source: DataSourceReader, dest: Destination):
    if dest.exists():
        source.get_local_delta_ops(dest).vacuum()


def write_db_to_delta(
    source: DataSourceReader | str,
    table: tuple[str, str],
    destination: Destination | Path,
    write_config: WriteConfig | None = None,
):
    if write_config is None:
        write_config = WriteConfig()
    if isinstance(destination, Path):
        from .destination.file_system import FileSystemDestination

        destination = cast(Destination, FileSystemDestination(destination))
    if isinstance(source, str):
        from .reader.odbc_reader import ODBCReader

        source = ODBCReader(source)
    delta_path = destination / "delta"
    cols = get_columns(source, table, dialect=write_config.dialect)

    (destination / "meta").mkdir()
    (destination / "meta/schema.json").upload_str(
        json.dumps([c.dict() for c in cols], indent=4)
    )
    if (destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION).exists():
        last_version_pk = source.get_local_delta_ops(
            destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
        ).version()
    else:
        last_version_pk = None
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
        lock_file_path.upload_str("")

        delta_col = (
            next((c for c in cols if c.column_name == write_config.delta_col))
            if write_config.delta_col
            else get_delta_col(cols)
        )
        pks = write_config.primary_keys or get_primary_keys(
            source, table, dialect=write_config.dialect
        )
        pk_cols = [c for c in cols if c.column_name in pks]
        assert len(pks) == len(pk_cols), f"Primary keys not found: {pks}"
        if (
            not (delta_path / "_delta_log").exists()
            or write_config.load_mode == "overwrite"
        ):
            delta_path.mkdir()
            do_full_load(
                source,
                table,
                delta_path,
                mode="overwrite",
                cols=cols,
                pk_cols=pk_cols,
                delta_col=delta_col,
                write_config=write_config,
            )
        else:
            if (
                delta_col is None
                or len(pks) == 0
                or write_config.load_mode == "force_full"
            ):
                do_full_load(
                    source,
                    table,
                    delta_path,
                    mode="append",
                    cols=cols,
                    pk_cols=pk_cols,
                    delta_col=delta_col,
                    write_config=write_config,
                )
            else:
                do_delta_load(
                    source,
                    table,
                    destination=destination,
                    delta_col=delta_col,
                    cols=cols,
                    pk_cols=pk_cols,
                    write_config=write_config,
                )
        lock_file_path.remove()
        _vacuum(
            source, destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
        )
        _vacuum(source, destination / "delta_load" / DBDeltaPathConfigs.DELTA_1_NAME)
        _vacuum(source, destination / "delta_load" / DBDeltaPathConfigs.DELTA_2_NAME)
        _vacuum(source, destination / "delta_load" / DBDeltaPathConfigs.PRIMARY_KEYS_TS)
    except Exception as e:
        # restore files
        if lock_file_path.exists():
            lock_file_path.remove()
        if last_version_pk is not None:
            o = source.get_local_delta_ops(
                destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            )
            if o.version() > last_version_pk:
                o.restore(last_version_pk)
        raise e


def restore_last_pk(
    reader: DataSourceReader,
    table: table_name_type,
    destination: Destination,
    delta_col: InformationSchemaColInfo,
    pk_cols: list[InformationSchemaColInfo],
    write_config: WriteConfig,
):
    delta_path = destination / "delta"
    reader.local_register_update_view(delta_path, _temp_table(table))

    sq_valid_from = reader.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(_temp_table(table)))
        .select(ex.func("max", ex.column(VALID_FROM_COL_NAME)).as_(VALID_FROM_COL_NAME))
        .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
    )
    if sq_valid_from is None or len(sq_valid_from) == 0:
        return False
    latest_full_load_date = sq_valid_from[0][VALID_FROM_COL_NAME]
    reader.local_register_view(
        sg.from_(ex.table_(ex.to_identifier(_temp_table(table)), alias="tr"))
        .select(
            *_get_cols_select(
                cols=pk_cols + [delta_col],
                table_alias="tr",
                source_uses_compat=True,
                data_type_map=write_config.data_type_map,
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
                source_uses_compat=True,
                data_type_map=write_config.data_type_map,
            )
        )
        .where(ex.column(VALID_FROM_COL_NAME) > ex.convert(latest_full_load_date))
    )
    sq = sq.qualify(
        ex.EQ(
            this=ex.Window(
                this=ex.RowNumber(),
                partition_by=[ex.column(pk.compat_name) for pk in pk_cols],
                order=ex.Order(
                    expressions=[
                        ex.Ordered(
                            this=ex.column(delta_col.compat_name),
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
                            ex.column(c.compat_name, "f").eq(
                                ex.column(c.compat_name, "d")
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
    cnt = reader.local_execute_sql_to_py(count_limit_one("v_last_pk_version"))[0]["cnt"]
    if cnt == 0:
        return False
    reader.local_execute_sql_to_delta(
        sq.from_("v_last_pk_version").select(ex.Star()),
        destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        mode="overwrite",
    )
    return True


def create_replace_view(
    reader: DataSourceReader,
    name: str,
    base_destination: Destination,
    *,
    version: int | None = None,
):
    reader.local_register_update_view(
        base_destination / f"delta_load/{name}", name, version=version
    )


def write_latest_pk(
    reader: DataSourceReader,
    destination: Destination,
    pks: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo,
    write_config: WriteConfig,
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
                    source_uses_compat=True,
                )
            ).from_(table_from_tuple("delta_2", alias="au")),
            (
                ex.select(
                    *_get_cols_select(
                        cols=pks + [delta_col],
                        table_alias="d1",
                        source_uses_compat=True,
                    )
                )
                .from_(ex.table_(DBDeltaPathConfigs.DELTA_1_NAME, alias="d1"))
                .join(
                    ex.table_("delta_2", alias="au2"),
                    ex.and_(
                        *[
                            ex.column(c.compat_name, "d1").eq(
                                ex.column(c.compat_name, "au2")
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
                        source_uses_compat=True,
                    )
                )
                .from_(ex.table_(DBDeltaPathConfigs.PRIMARY_KEYS_TS, alias="cpk"))
                .join(
                    ex.table_("delta_2", alias="au3"),
                    ex.and_(
                        *[
                            ex.column(c.compat_name, "cpk").eq(
                                ex.column(c.compat_name, "au3")
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
                            ex.column(c.compat_name, "cpk").eq(
                                ex.column(c.compat_name, "au4")
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


def _temp_table(table: table_name_type):
    if isinstance(table, str):
        return "temp_" + table
    return "temp_" + "_".join(table)


def do_delta_load(
    reader: DataSourceReader,
    table: table_name_type,
    destination: Destination,
    *,
    delta_col: InformationSchemaColInfo,
    cols: list[InformationSchemaColInfo],
    pk_cols: list[InformationSchemaColInfo],
    write_config: WriteConfig,
):
    last_pk_path = destination / f"delta_load/{DBDeltaPathConfigs.LATEST_PK_VERSION}"
    logger.info(
        f"{table}: Start Delta Load with Delta Column {delta_col.column_name} and pks: {', '.join((c.column_name for c in pk_cols))}"
    )

    if not (last_pk_path / "_delta_log").exists():  # or do a full load?
        logger.warning(f"{table}: Primary keys missing, try to restore")
        try:
            restore_sucess = restore_last_pk(
                reader,
                table,
                destination,
                delta_col,
                pk_cols,
                write_config=write_config,
            )
        except Exception as e:
            logger.warning(f"{table}: Could not restore primary keys: {e}")
            restore_sucess = False
        if not restore_sucess:
            logger.warning(f"{table}: No primary keys found, do a full load")
            do_full_load(
                reader,
                table,
                delta_path=destination / "delta",
                mode="append",
                cols=cols,
                pk_cols=pk_cols,
                delta_col=delta_col,
                write_config=write_config,
            )
            return
    old_pk_version = reader.get_local_delta_ops(
        destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
    ).version()
    delta_path = destination / "delta"
    reader.local_register_update_view(delta_path, _temp_table(table))
    delta_load_value = reader.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(_temp_table(table))).select(
            ex.func(
                "MAX",
                _cast(
                    delta_col.compat_name,
                    delta_col.data_type,
                ),
            ).as_("max_ts")
        )
    )[0]["max_ts"]

    if delta_load_value is None:
        logger.warning(f"{table}: No delta load value, do a full load")
        do_full_load(
            reader,
            table,
            delta_path,
            mode="append",
            cols=cols,
            pk_cols=pk_cols,
            delta_col=delta_col,
            write_config=write_config,
        )
        return
    logger.info(
        f"{table}: Start delta step 1, get primary keys and timestamps. MAX({delta_col.column_name}): {delta_load_value}"
    )
    _retrieve_primary_key_data(
        reader=reader,
        table=table,
        delta_col=delta_col,
        pk_cols=pk_cols,
        destination=destination,
        write_config=write_config,
    )

    criterion = _cast(
        delta_col.column_name,
        delta_col.data_type,
        table_alias="t",
        type_map=write_config.data_type_map,
    ) > ex.convert(delta_load_value)
    logger.info(f"{table}: Start delta step 2, load updates by timestamp")
    _load_updates_to_delta(
        reader,
        sql=_get_update_sql(
            cols=cols, criterion=criterion, table=table, write_config=write_config
        ),
        delta_path=delta_path,
        delta_name="delta_1",
        write_config=write_config,
    )

    _handle_additional_updates(
        reader=reader,
        table=table,
        delta_path=delta_path,
        pk_cols=pk_cols,
        delta_col=delta_col,
        cols=cols,
        write_config=write_config,
        old_pk_version=old_pk_version,
    )
    reader.local_register_update_view(delta_path, _temp_table(table))

    logger.info(f"{table}: Start delta step 3.5, write meta for next delta load")

    write_latest_pk(reader, destination, pk_cols, delta_col, write_config=write_config)

    logger.info(f"{table}: Start delta step 4.5, write deletes")
    do_deletes(
        reader=reader,
        destination=destination,
        cols=cols,
        pk_cols=pk_cols,
        old_pk_version=old_pk_version,
        write_config=write_config,
    )
    logger.info(f"{table}: Done delta load")


def do_deletes(
    reader: DataSourceReader,
    destination: Destination,
    # delta_table: DeltaTable,
    cols: list[InformationSchemaColInfo],
    pk_cols: list[InformationSchemaColInfo],
    old_pk_version: int,
    write_config: WriteConfig,
):
    reader.local_register_update_view(
        destination / f"delta_load/{ DBDeltaPathConfigs.LATEST_PK_VERSION}",
        DBDeltaPathConfigs.LATEST_PK_VERSION,
    )
    LAST_PK_VERSION = "LAST_PK_VERSION"
    reader.local_register_update_view(
        destination / f"delta_load/{ DBDeltaPathConfigs.LATEST_PK_VERSION}",
        LAST_PK_VERSION,
        version=old_pk_version,
    )
    delete_query = ex.except_(
        left=ex.select(
            *_get_cols_select(
                pk_cols,
                table_alias="lpk",
                source_uses_compat=True,
            )
        ).from_(table_from_tuple(LAST_PK_VERSION, alias="lpk")),
        right=ex.select(
            *_get_cols_select(
                pk_cols,
                table_alias="cpk",
                source_uses_compat=True,
            )
        ).from_(table_from_tuple(DBDeltaPathConfigs.LATEST_PK_VERSION, alias="cpk")),
    )

    non_pk_cols = [c for c in cols if c not in pk_cols]
    non_pk_select = [ex.Null().as_(c.compat_name) for c in non_pk_cols]
    deletes_with_schema = union(
        [
            ex.select(
                *_get_cols_select(
                    pk_cols,
                    table_alias="d1",
                    source_uses_compat=True,
                )
            )
            .select(
                *_get_cols_select(
                    non_pk_cols,
                    table_alias="d1",
                    source_uses_compat=True,
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
                ex.Column(this=ex.Star(), table=ex.Identifier(this="d", quoted=False))
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
    ).with_("deletes", as_=delete_query)
    reader.local_register_view(deletes_with_schema, "deletes_with_schema")
    has_deletes = (
        reader.local_execute_sql_to_py(count_limit_one("deletes_with_schema"))[0]["cnt"]
        > 0
    )
    if has_deletes:
        reader.local_execute_sql_to_delta(
            sg.from_("deletes_with_schema").select("*"),
            destination / "delta",
            mode="append",
        )


def _retrieve_primary_key_data(
    reader: DataSourceReader,
    table: table_name_type,
    delta_col: InformationSchemaColInfo,
    pk_cols: list[InformationSchemaColInfo],
    destination: Destination,
    write_config: WriteConfig,
):
    pk_ts_col_select = ex.select(
        *_get_cols_select(
            is_full=None,
            is_deleted=None,
            cols=pk_cols + [delta_col],
            with_valid_from=False,
            data_type_map=write_config.data_type_map,
            source_uses_compat=False,
        )
    ).from_(table_from_tuple(table))
    pk_ts_reader_sql = pk_ts_col_select.sql(write_config.dialect)

    pk_path = destination / f"delta_load/{DBDeltaPathConfigs.PRIMARY_KEYS_TS}"

    reader.source_write_sql_to_delta(
        sql=pk_ts_reader_sql, delta_path=pk_path, mode="overwrite"
    )
    return pk_path


T = TypeVar("T")


def _list_to_chunks(input: Iterable[T], chunk_size: int):
    chunk: list[T] = list()
    for item in input:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = list()
    if len(chunk) > 0:
        yield chunk


def _handle_additional_updates(
    reader: DataSourceReader,
    table: table_name_type,
    delta_path: Destination,
    pk_cols: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo,
    cols: list[InformationSchemaColInfo],
    write_config: WriteConfig,
    old_pk_version: int,
):
    """Handles updates that are not logical by their timestamp. This can happen on a restore from backup, for example."""
    folder = delta_path.parent
    pk_ds_cols = pk_cols + [delta_col]
    reader.local_register_update_view(
        folder / f"delta_load/{ DBDeltaPathConfigs.PRIMARY_KEYS_TS}",
        DBDeltaPathConfigs.PRIMARY_KEYS_TS,
    )
    LAST_PK_VERSION = "LAST_PK_VERSION"
    reader.local_register_update_view(
        folder / f"delta_load/{ DBDeltaPathConfigs.LATEST_PK_VERSION}",
        LAST_PK_VERSION,
        version=old_pk_version,
    )

    reader.local_register_view(
        ex.except_(
            left=ex.select(
                *_get_cols_select(
                    cols=pk_ds_cols,
                    table_alias="pk",
                    source_uses_compat=True,
                )
            ).from_(ex.table_(DBDeltaPathConfigs.PRIMARY_KEYS_TS, alias="pk")),
            right=ex.select(
                *_get_cols_select(
                    cols=pk_ds_cols,
                    table_alias="lpk",
                    source_uses_compat=True,
                )
            ).from_(table_from_tuple(LAST_PK_VERSION, alias="lpk")),
        ),
        "additional_updates",
    )

    sql_query = ex.except_(
        left=ex.select(
            *_get_cols_select(
                cols=pk_cols,
                table_alias="au",
                source_uses_compat=True,
            )
        ).from_(ex.table_("additional_updates", alias="au")),
        right=ex.select(
            *_get_cols_select(
                cols=pk_cols,
                table_alias="d1",
                source_uses_compat=True,
            )
        ).from_(table_from_tuple("delta_1", alias="d1")),
    )
    reader.local_register_view(sql_query, "real_additional_updates")
    update_count: int = reader.local_execute_sql_to_py(
        sg.from_("real_additional_updates").select(ex.Count(this=ex.Star()).as_("cnt"))
    )[0]["cnt"]

    from .sql_schema import get_sql_type
    from .query import sql_quote_value

    jsd = reader.local_execute_sql_to_py(
        sg.from_("real_additional_updates").select(
            *[
                ex.column(c.compat_name).as_("p" + str(i), quoted=False)
                for i, c in enumerate(pk_cols)
            ]
        )
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
            return "COLLATE Latin1_General_100_BIN "
        return ""

    delta_2_path = folder / "delta_load/delta_2"

    def full_sql(js: str):
        col_defs = ", ".join(
            [
                f"p{i} {get_sql_type(p.data_type, p.character_maximum_length)}"
                for i, p in enumerate(pk_cols)
            ]
        )

        selects = list(
            _get_cols_select(
                cols,
                is_full=False,
                is_deleted=False,
                with_valid_from=True,
                table_alias="t",
                data_type_map=write_config.data_type_map,
                source_uses_compat=False,
            )
        )
        sql = (
            ex.select(*selects)
            .from_(table_from_tuple(table, alias="t"))
            .sql(write_config.dialect)
        )
        pk_map = ", ".join(
            [
                "p" + str(i) + " as " + sql_quote_name(c.compat_name)
                for i, c in enumerate(pk_cols)
            ]
        )
        return f"""{sql}
        inner join (SELECT {pk_map} FROM OPENJSON({sql_quote_value(js)}) with ({col_defs}) ) ttt
             on {' AND '.join([f't.{sql_quote_name(c.column_name)} {_collate(c)} = ttt.{sql_quote_name(c.compat_name)}' for c in pk_cols])}
        """

    if update_count == 0:
        reader.source_write_sql_to_delta(full_sql("[]"), delta_2_path, mode="overwrite")
    elif (
        update_count > 1000
    ):  # many updates. get the smallest timestamp and do "normal" delta, even if there are too many records then

        logger.warning(
            f"{table}: Start delta step 3, load {update_count} strange updates via normal delta load"
        )
        delta_load_value = reader.local_execute_sql_to_py(
            ex.select(
                ex.func("MIN", ex.column(delta_col.compat_name)).as_("min_ts")
            ).from_(ex.table_("additional_updates", alias="rau"))
        )[0]["min_ts"]
        criterion = _cast(
            delta_col.column_name,
            delta_col.data_type,
            table_alias="t",
            type_map=write_config.data_type_map,
        ) > ex.convert(delta_load_value)
        logger.info(f"{table}: Start delta step 2, load updates by timestamp")
        _load_updates_to_delta(
            reader,
            sql=_get_update_sql(
                cols=cols, criterion=criterion, table=table, write_config=write_config
            ),
            delta_path=delta_path,
            delta_name="delta_1",
            write_config=write_config,
        )
    else:
        first = True
        # we don't want to overshoot 8000 chars here because of spark. we estimate how much space in json a record of pk's will take

        char_size_pks = sum(
            [
                5
                + (
                    4
                    if p.data_type
                    in ["bit", "int", "bigint", "tinyint", "bool", "smallint"]
                    else 10
                )
                for p in pk_cols
            ]
        )
        batch_size = max(10, int(7000 / char_size_pks))

        logger.warning(
            f"{table}: Start delta step 3, load {update_count} strange updates via {min(1,int(len(jsd)/batch_size))} batches"
        )
        for chunk in _list_to_chunks(jsd, batch_size):

            reader.source_write_sql_to_delta(
                full_sql(json.dumps(chunk)),
                delta_2_path,
                mode="overwrite" if first else "append",
            )
            first = False
        reader.local_register_update_view(delta_2_path, "delta_2")
        reader.local_execute_sql_to_delta(
            sg.from_("delta_2").select(ex.Star()),
            delta_path,
            mode="append",
        )


def _get_update_sql(
    cols: list[InformationSchemaColInfo],
    criterion: str | Sequence[str | ex.Expression] | ex.Expression,
    table: table_name_type,
    write_config: WriteConfig,
):
    if isinstance(criterion, ex.Expression):
        criterion = [criterion]
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
                data_type_map=write_config.data_type_map,
                source_uses_compat=False,
            )
        )
        .where(
            *(criterion if not isinstance(criterion, str) else []),
            dialect=write_config.dialect,
        )
        .from_(table_from_tuple(table, alias="t"))
        .sql(write_config.dialect)
    )
    if isinstance(criterion, str):
        delta_sql += " " + criterion
    return delta_sql


def _load_updates_to_delta(
    reader: DataSourceReader,
    delta_path: Destination,
    sql: str | ex.Query,
    delta_name: str,
    write_config: WriteConfig,
):
    if isinstance(sql, ex.Query):
        sql = sql.sql(write_config.dialect)

    delta_name_path = delta_path.parent / f"delta_load/{delta_name}"
    logger.info(f"Executing {sql}")
    reader.source_write_sql_to_delta(sql, delta_name_path, mode="overwrite")
    reader.local_register_update_view(delta_name_path, delta_name)
    count = reader.local_execute_sql_to_py(count_limit_one(delta_name))[0]["cnt"]
    if count == 0:
        return
    reader.local_execute_sql_to_delta(
        sg.from_(delta_name).select(ex.Star()), delta_path, mode="append"
    )


def do_full_load(
    reader: DataSourceReader,
    table: table_name_type,
    delta_path: Destination,
    mode: Literal["overwrite", "append"],
    cols: list[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo | None,
    pk_cols: list[InformationSchemaColInfo],
    write_config: WriteConfig,
):
    logger.info(f"{table}: Start Full Load")
    sql = (
        ex.select(
            *_get_cols_select(
                is_deleted=False,
                is_full=True,
                cols=cols,
                with_valid_from=True,
                data_type_map=write_config.data_type_map,
                source_uses_compat=False,
            )
        )
        .from_(table_from_tuple(table))
        .sql(write_config.dialect)
    )
    if (delta_path / "_delta_log").exists():
        reader.local_register_update_view(delta_path, _temp_table(table))
        res = reader.local_execute_sql_to_py(
            sg.from_(ex.to_identifier(_temp_table(table))).select(
                ex.func("max", ex.column(VALID_FROM_COL_NAME)).as_(VALID_FROM_COL_NAME)
            )
        )
        max_valid_from = res[0][VALID_FROM_COL_NAME] if res else None
    else:
        max_valid_from = None
    reader.source_write_sql_to_delta(sql, delta_path, mode=mode)
    if delta_col is None:
        logger.info(f"{table}: Full Load done")
        return
    logger.info(f"{table}: Full Load done, write meta for delta load")

    reader.local_register_update_view(delta_path, _temp_table(table))
    (delta_path.parent / "delta_load").mkdir()
    query = sg.from_(ex.to_identifier(_temp_table(table))).select(
        *(
            [ex.column(pk.compat_name) for pk in pk_cols]
            + ([ex.column(delta_col.compat_name)] if delta_col else [])
        )
    )
    if max_valid_from:
        query = query.where(ex.column(VALID_FROM_COL_NAME) > ex.convert(max_valid_from))
    reader.local_execute_sql_to_delta(
        query,
        delta_path.parent / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        mode="overwrite",
    )
