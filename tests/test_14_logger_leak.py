"""Reproduces a bug in DeltaStorageBackend (delta_logger.py):

`_pending_logs` is declared as a class-level attribute with a mutable
default (`list[LogMessage] = []`) and is never assigned in `__init__`.
Because of that, every `DeltaStorageBackend` instance shares the exact
same list object. Since one `DeltaStorageBackend` is created per
`DeltaLogger`, and one `DeltaLogger` is created per table/write, running
writes for two different tables in the same process means their log
messages accumulate in ONE shared list - whichever instance flushes
first writes ALL pending messages (including ones belonging to a
different table's logger) to its own log_file_path.
"""
from datetime import datetime, timezone
from pathlib import Path

from odbc2deltalake.destination.file_system import FileSystemDestination
from odbc2deltalake.delta_logger import DeltaStorageBackend
from odbc2deltalake.logging import LogMessage


def _msg(text: str) -> LogMessage:
    return LogMessage(
        message=text,
        type="info",
        date=datetime.now(tz=timezone.utc),
        logger_id="id",
        logger_name="test",
    )


def test_pending_logs_are_not_shared_between_instances(tmp_path: Path):
    dest_a = FileSystemDestination(tmp_path / "table_a" / "log")
    dest_b = FileSystemDestination(tmp_path / "table_b" / "log")

    # source is only used inside flush(), which we never trigger here
    backend_a = DeltaStorageBackend(dest_a, source=None)  # type: ignore[arg-type]
    backend_b = DeltaStorageBackend(dest_b, source=None)  # type: ignore[arg-type]

    msg_a = _msg("message for table a")
    backend_a.log(msg_a)

    # each backend must track its own pending logs independently - table b's
    # backend must NOT see table a's message
    assert backend_a._pending_logs == [msg_a]
    assert backend_b._pending_logs == [], (
        "DeltaStorageBackend._pending_logs is shared across instances "
        "(class-level mutable default), so a message logged on one "
        "instance leaks into every other instance."
    )
