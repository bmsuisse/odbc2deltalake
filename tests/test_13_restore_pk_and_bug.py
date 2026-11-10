"""Reproduces a bug in create_last_pk_version_view (write_utils/restore_pk.py):

The "last_full_load" view's WHERE clause is built with the Python keyword
`and` between two sqlglot Expression objects instead of `&` / `ex.and_()`.
Since sqlglot expressions are always truthy, `expr1 and expr2` evaluates to
plain Python `and` semantics and just returns `expr2` - `expr1` is silently
dropped. This means the `__is_full_load = True` condition never makes it
into the SQL, so the view can also pick up rows that are NOT part of the
full load, as long as their __timestamp matches the last full load's
timestamp exactly - which produces two rows for the same primary key in the
supposedly-single-row-per-pk "last pk version" output.
"""
from pathlib import Path
from datetime import datetime, timezone
import shutil

from odbc2deltalake.destination.file_system import FileSystemDestination
from odbc2deltalake.reader.odbc_reader import ODBCReader
from odbc2deltalake.write_init import WriteConfig, WriteConfigAndInfos
from odbc2deltalake.metadata import InformationSchemaColInfo
from odbc2deltalake.delta_logger import DeltaLogger
from odbc2deltalake.write_utils.restore_pk import create_last_pk_version_view
import sqlglot.expressions as ex


def _col(name: str) -> InformationSchemaColInfo:
    return InformationSchemaColInfo(
        column_name=name, data_type=ex.DataType.build("bigint"), data_type_str="bigint"
    )


def test_last_full_load_view_ignores_non_full_load_rows(tmp_path: Path):
    dest = FileSystemDestination(tmp_path / "dest")
    reader = ODBCReader("unused", local_db=":memory:", source_dialect="tsql")

    pk_col = _col("id")
    delta_col = _col("ts")

    t = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)  # shared timestamp
    rows = [
        # the real full load: two rows, pks 1 and 2
        {"id": 1, "ts": 100, "__timestamp": t, "__is_deleted": False, "__is_full_load": True},
        {"id": 2, "ts": 100, "__timestamp": t, "__is_deleted": False, "__is_full_load": True},
        # NOT part of the full load, but happens to share the exact same
        # __timestamp (e.g. two statements in the same transaction) - must
        # be excluded from the "last_full_load" snapshot
        {"id": 3, "ts": 999, "__timestamp": t, "__is_deleted": False, "__is_full_load": False},
    ]
    reader.local_pylist_to_delta(rows, dest / "delta", mode="append")

    infos = WriteConfigAndInfos(
        col_infos=[pk_col, delta_col],
        pk_cols=[pk_col],
        delta_col=delta_col,
        write_config=WriteConfig(),
        destination=dest,
        source=reader,
        table_or_query=("dbo", "t"),
        logger=DeltaLogger(dest / "log", reader),
    )

    _, view_name, success = create_last_pk_version_view(infos, view_prefix="v_test_")
    assert success and view_name

    result = reader.local_execute_sql_to_py(ex.select("*").from_(view_name))
    pks_returned = sorted(r["id"] for r in result)

    # only pks 1 and 2 were part of the actual full load
    assert pks_returned == [1, 2], (
        f"expected only the full-load rows [1, 2], got {pks_returned}: "
        "row for pk=3 (__is_full_load=False) leaked into the full-load "
        "snapshot because the `is_full_load` filter is silently dropped "
        "by the `and` bug in restore_pk.py"
    )
    shutil.rmtree(tmp_path, ignore_errors=True)
