from dataclasses import dataclass
import dataclasses
from pathlib import Path
from typing import Callable, Literal, Mapping, Sequence, TypeVar, Union, cast
import sqlglot as sg
from odbc2deltalake.destination.destination import (
    Destination,
)
from odbc2deltalake.reader import DataSourceReader
from .metadata import (
    get_primary_keys,
    get_columns,
    InformationSchemaColInfo,
)
import sqlglot.expressions as ex
from .sql_glot_utils import table_from_tuple
import logging
from .delta_logger import DeltaLogger


IS_DELETED_COL_NAME = "__is_deleted"
VALID_FROM_COL_NAME = "__timestamp"
IS_FULL_LOAD_COL_NAME = "__is_full_load"


T = TypeVar("T")

_default_type_map = {
    "datetime": ex.DataType(this="datetime2(6)"),
    "datetime2": ex.DataType(this="datetime2(6)"),
    "rowversion": ex.DataType.Type.BIGINT,
    "timestamp": ex.DataType.Type.BIGINT,
}
DEFAULT_DATA_TYPE_MAP: Mapping[str, ex.DataType] = _default_type_map


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

    primary_keys: Union[list[str], None] = None
    """A list of primary keys to use for the delta load. If None, the primary keys will be determined from the source"""

    delta_col: Union[str, None] = None
    """The column to use for the delta load. If None, the column will be determined from the source. Should be mostly increasing to make load efficient"""

    load_mode: Literal[
        "overwrite",
        "append",
        "force_full",
        "append_inserts",
        "simple_delta",
        "simple_delta_check",
    ] = "append"
    """The load mode to use. Attention: overwrite will not help you build scd2, the history is in the delta table only
        append_inserts is for when you have a delta column which is strictly increasing and you want to append new rows only. No deletes of rows. might be good for logs
        simple_delta is for sources where the delta col is a datetime and you can be sure that there are no deletes or additional updates
        simple_delta_check is like simple_delta, but checks for deletes if the count does not match. Only use if you do not expect frequent deletes, as it will do simple_delta AND delta if there are deletes, which is slower than delta
    """

    data_type_map: Mapping[str, ex.DataType] = dataclasses.field(
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
    delta_col: Union[InformationSchemaColInfo, None]
    write_config: WriteConfig
    destination: Destination
    source: DataSourceReader
    table_or_query: Union[ex.Query, tuple[str, str], str]
    logger: DeltaLogger

    def execute(self):
        from .db_to_delta import exec_write_db_to_delta

        exec_write_db_to_delta(self)

    def from_(self, alias: str) -> ex.Select:
        if isinstance(self.table_or_query, ex.Query):
            return sg.from_(self.table_or_query.subquery().as_(alias))
        return sg.from_(table_from_tuple(self.table_or_query, alias))


def get_delta_col(
    cols: Sequence[InformationSchemaColInfo], dialect: str
) -> Union[InformationSchemaColInfo, None]:
    row_start_col: Union[InformationSchemaColInfo, None] = None
    for c in cols:
        if dialect == "tsql" and c.data_type.this in [
            ex.DataType.Type.ROWVERSION,
            ex.DataType.Type.TIMESTAMP,
        ]:
            return c
        if c.generated_always_type_desc == "AS_ROW_START":
            row_start_col = c
        if c.column_name == "__timestamp":
            return c
    return row_start_col


def make_writer(
    source: Union[DataSourceReader, str],
    table_or_query: Union[str, tuple[str, str], ex.Query],
    destination: Union[Destination, Path],
    write_config: Union[WriteConfig, None] = None,
):
    if write_config is None:
        write_config = WriteConfig()
    if isinstance(destination, Path):
        from .destination.file_system import FileSystemDestination

        destination = cast(Destination, FileSystemDestination(destination))
    if isinstance(source, str):
        from .reader.odbc_reader import ODBCReader

        source = ODBCReader(source)
    cols = get_columns(source, table_or_query, dialect=write_config.dialect)

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
        delta_col = get_delta_col(cols, write_config.dialect)

    _pks = write_config.primary_keys
    if _pks is None and (
        isinstance(table_or_query, str) or isinstance(table_or_query, tuple)
    ):
        _pks = get_primary_keys(source, table_or_query, dialect=write_config.dialect)
    elif _pks is None:
        _pks = []
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
        table_or_query=table_or_query,
    )
