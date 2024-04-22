from dataclasses import dataclass
import dataclasses
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Literal, Mapping, Sequence, TypeVar, cast
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
import pydantic
from .delta_logger import DeltaLogger

is_pydantic_2 = int(pydantic.__version__.split(".")[0]) > 1


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


def compat_name(inf: InformationSchemaColInfo) -> str:
    invalid_chars = " ,;{}()\n\t="
    res = inf.column_name
    for ic in invalid_chars:
        res = res.replace(ic, "_")
    return res


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


@dataclass(frozen=True)
class WriteConfig:

    dialect: str = "tsql"
    """The sqlglot dialect to use for the SQL generation against the source"""

    primary_keys: list[str] | None = None
    """A list of primary keys to use for the delta load. If None, the primary keys will be determined from the source"""

    delta_col: str | None = None
    """The column to use for the delta load. If None, the column will be determined from the source. Should be mostly increasing to make load efficient"""

    load_mode: Literal[
        "overwrite", "append", "force_full", "append_inserts", "simple_delta"
    ] = "append"
    """The load mode to use. Attention: overwrite will not help you build scd2, the history is in the delta table only
        append_inserts is for when you have a delta column which is strictly increasing and you want to append new rows only. No deletes of rows. might be good for logs
        simple_delta is for sources where the delta col is a datetime and you can be sure that there are no deletes or additional updates
    """

    data_type_map: Mapping[str, ex.DATA_TYPE] = dataclasses.field(
        default_factory=lambda: _default_type_map.copy()
    )
    """Set this if you want to map stuff like decimal to double before writing to delta. We recommend doing so later in ETL usually"""

    no_complex_entries_load: bool = False
    """If true, will not load 'strange updates' via OPENJSON. Use if your db does not support OPENJSON or you're fine to get some additional updates in order to reduce complexity"""

    get_target_name: Callable[[InformationSchemaColInfo], str] = dataclasses.field(
        default_factory=lambda: compat_name
    )
    """A method that returns the target name of a column. This is used to map the source column names to the target column names.
    Use if you want to apply some naming convention or avoid special characters in the target. """


@dataclass(frozen=True)
class WriteConfigAndInfos:
    col_infos: Sequence[InformationSchemaColInfo]
    pk_cols: Sequence[InformationSchemaColInfo]
    delta_col: InformationSchemaColInfo | None
    write_config: WriteConfig
    destination: Destination
    source: DataSourceReader
    table: table_name_type
    logger: DeltaLogger

    def execute(self):
        from .db_to_delta import exec_write_db_to_delta

        exec_write_db_to_delta(self)


def get_delta_col(
    cols: Sequence[InformationSchemaColInfo],
) -> InformationSchemaColInfo | None:
    row_start_col: InformationSchemaColInfo | None = None
    for c in cols:
        if c.data_type.lower() in ["rowversion", "timestamp"]:
            return c
        if c.generated_always_type_desc == "AS_ROW_START":
            row_start_col = c
        if c.column_name == "__timestamp":
            return c
    return row_start_col


def make_writer(
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
    cols = get_columns(source, table, dialect=write_config.dialect)

    if write_config.delta_col:
        delta_col = next(
            (c for c in cols if c.column_name == write_config.delta_col), None
        )
        if delta_col is None:
            delta_col = next(
                (
                    c
                    for c in cols
                    if write_config.get_target_name(c) == write_config.delta_col
                ),
                None,
            )
        if delta_col is None:
            raise ValueError(
                f"Delta column {write_config.delta_col} not found in source"
            )
    else:
        delta_col = get_delta_col(cols)

    _pks = write_config.primary_keys or get_primary_keys(
        source, table, dialect=write_config.dialect
    )
    pk_cols: Sequence[InformationSchemaColInfo] = []
    for pk in _pks:
        pk_col = next((c for c in cols if c.column_name == pk), None)
        if pk_col is None:
            pk_col = next(
                (c for c in cols if write_config.get_target_name(c) == pk), None
            )
        if pk_col is None:
            raise ValueError(f"Primary key {pk} not found in source")
        pk_cols.append(pk_col)
    assert len(_pks) == len(pk_cols), f"Primary keys not found: {_pks}"
    return WriteConfigAndInfos(
        cols,
        pk_cols=pk_cols,
        delta_col=delta_col,
        write_config=write_config,
        destination=destination,
        source=source,
        logger=DeltaLogger(destination / "log", source, logging.getLogger(__name__)),
        table=table,
    )
