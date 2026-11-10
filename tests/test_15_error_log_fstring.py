"""Reproduces a bug in exec_write_db_to_delta (db_to_delta.py):

The except-block logs the error with `"Error during load: {e}"` -
missing the `f` prefix - so `{e}` is never interpolated and every
error-path log entry literally contains the text "Error during load:
{e}" instead of the actual exception message. This degrades
observability exactly when an operator most needs it (a failed load).

This test forces exec_write_db_to_delta to hit its `except Exception`
branch (by making local_delta_table_exists raise once the code is past
the initial setup) and then reads back the persisted error log entry to
check that the real exception message made it into the log message.
"""
from pathlib import Path

import pytest

from odbc2deltalake.destination.file_system import FileSystemDestination
from odbc2deltalake.reader.odbc_reader import ODBCReader
from odbc2deltalake.write_init import WriteConfig, WriteConfigAndInfos
from odbc2deltalake.metadata import InformationSchemaColInfo
from odbc2deltalake.delta_logger import DeltaLogger
from odbc2deltalake.db_to_delta import exec_write_db_to_delta
from odbc2deltalake.destination.destination import Destination
import sqlglot.expressions as ex


def _col(name: str) -> InformationSchemaColInfo:
    return InformationSchemaColInfo(
        column_name=name, data_type=ex.DataType.build("bigint"), data_type_str="bigint"
    )


class _BoomOnDeltaPathReader(ODBCReader):
    """A reader that behaves normally, except it raises once
    exec_write_db_to_delta asks whether the *main* delta table exists -
    simulating a failure deep inside a real load, without needing a real
    ODBC connection."""

    def local_delta_table_exists(
        self, delta_path: Destination, extended_check: bool = False
    ) -> bool:
        if str(delta_path).rstrip("/").endswith("/delta"):
            raise RuntimeError("boom - simulated failure during load")
        return super().local_delta_table_exists(delta_path, extended_check)


def test_error_log_contains_actual_exception_message(tmp_path: Path):
    dest = FileSystemDestination(tmp_path / "dest")
    reader = _BoomOnDeltaPathReader("unused", local_db=":memory:", source_dialect="tsql")

    pk_col = _col("id")

    infos = WriteConfigAndInfos(
        col_infos=[pk_col],
        pk_cols=[pk_col],
        delta_col=None,
        write_config=WriteConfig(),
        destination=dest,
        source=reader,
        table_or_query=("dbo", "t"),
        logger=DeltaLogger(dest / "log", reader),
    )

    with pytest.raises(RuntimeError, match="boom - simulated failure during load"):
        exec_write_db_to_delta(infos)

    reader.local_register_update_view(dest / "log", "v_log")
    rows = reader.local_execute_sql_to_py(
        ex.select("*").from_("v_log").where(ex.column("type").eq("error"))
    )
    assert len(rows) == 1
    message = rows[0]["message"]

    assert "{e}" not in message, (
        f"error log message was not f-string interpolated: {message!r}"
    )
    assert "boom - simulated failure during load" in message, (
        f"error log message does not contain the actual exception text: {message!r}"
    )
