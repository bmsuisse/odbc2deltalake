"""Reproduces a bug in do_append_inserts_load (db_to_delta.py):

`criterion = ... if delta_load_value else None` uses Python truthiness.
If the delta/identity column's current max value is legitimately `0`
(e.g. an identity/serial column whose first row has id 0),
delta_load_value is `0`, which is falsy, so criterion becomes None -
meaning `_get_update_sql` applies NO filter at all, and the
append-only load re-selects and re-appends EVERY row from the source
again, since append_inserts mode has no dedup (see
WriteConfig.load_mode docstring).

This test drives the real do_append_inserts_load code path, seeding a
local delta table whose max delta value is 0, and captures the SQL
that would be sent to the source (by overriding
source_write_sql_to_delta - the point where the query would otherwise
be executed over a real ODBC connection) to assert whether a `> 0`
filter made it into the WHERE clause.
"""
from pathlib import Path
from datetime import datetime, timezone

from odbc2deltalake.destination.file_system import FileSystemDestination
from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.reader.odbc_reader import ODBCReader
from odbc2deltalake.write_init import WriteConfig, WriteConfigAndInfos
from odbc2deltalake.metadata import InformationSchemaColInfo
from odbc2deltalake.delta_logger import DeltaLogger
from odbc2deltalake.db_to_delta import do_append_inserts_load
import sqlglot.expressions as ex


def _col(name: str) -> InformationSchemaColInfo:
    return InformationSchemaColInfo(
        column_name=name, data_type=ex.DataType.build("bigint"), data_type_str="bigint"
    )


class _StopForTest(Exception):
    """Raised to stop do_append_inserts_load right after it built the SQL,
    before it would try to hit a real (non-existent) ODBC connection."""


class _CapturingReader(ODBCReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.captured_sql = None

    def source_write_sql_to_delta(
        self,
        sql: str,
        delta_path: Destination,
        mode,
        *,
        allow_schema_drift,
    ):
        self.captured_sql = sql
        raise _StopForTest()


def test_append_inserts_zero_delta_value_still_filters(tmp_path: Path):
    dest = FileSystemDestination(tmp_path / "dest")
    reader = _CapturingReader("unused", local_db=":memory:", source_dialect="tsql")

    pk_col = _col("id")
    delta_col = _col("id")

    # seed the local "delta" table so MAX(id) == 0, the legitimate-zero case
    t = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        {
            "id": 0,
            "__timestamp": t,
            "__is_deleted": False,
            "__is_full_load": True,
        }
    ]
    reader.local_pylist_to_delta(rows, dest / "delta", mode="append")

    infos = WriteConfigAndInfos(
        col_infos=[pk_col],
        pk_cols=[pk_col],
        delta_col=delta_col,
        write_config=WriteConfig(load_mode="append_inserts"),
        destination=dest,
        source=reader,
        table_or_query=("dbo", "t"),
        logger=DeltaLogger(dest / "log", reader),
    )

    try:
        do_append_inserts_load(infos)
    except _StopForTest:
        pass

    assert reader.captured_sql is not None, "source_write_sql_to_delta was never called"
    assert "WHERE" in reader.captured_sql.upper(), (
        "do_append_inserts_load generated no WHERE filter when the delta "
        "value was legitimately 0 - it treated 0 as falsy/'no value' and "
        f"re-selected the entire source table.\nSQL was:\n{reader.captured_sql}"
    )
