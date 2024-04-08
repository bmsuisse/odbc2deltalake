from datetime import datetime, timezone
import json
from odbc2deltalake.destination.destination import Destination
from pydantic import BaseModel
import pydantic
import logging
from uuid import uuid4

from odbc2deltalake.reader.reader import DataSourceReader

is_pydantic_2 = int(pydantic.__version__.split(".")[0]) > 1


class LogMessage(BaseModel):
    message: str
    type: str
    date: datetime
    sql: str | None = None
    load: str | None = None
    sub_load: str | None = None
    error_trackback: str | None = None


class DeltaLogger:

    _pending_logs: list[LogMessage] = []

    def __init__(
        self,
        log_file_path: Destination,
        source: DataSourceReader,
        base_logger: logging.Logger | None = None,
    ):
        self.log_file_path = log_file_path
        self.base_logger = base_logger
        self.source = source
        self.id = uuid4()

    def info(
        self,
        message: str,
        *,
        load: str | None = None,
        sql: str | None = None,
        sub_load: str | None = None,
    ):
        self._pending_logs.append(
            LogMessage(
                message=message,
                type="info",
                date=datetime.now(tz=timezone.utc),
                load=load,
                sql=sql,
                sub_load=sub_load,
            )
        )
        if self.base_logger:
            self.base_logger.info(
                message if isinstance(message, str) else json.dumps(message)
            )

        if len(self._pending_logs) > 10:
            self.flush()

    def warning(
        self,
        message: str,
        *,
        load: str | None = None,
        sql: str | None = None,
        sub_load: str | None = None,
    ):
        self._pending_logs.append(
            LogMessage(
                message=message,
                type="warn",
                date=datetime.now(tz=timezone.utc),
                load=load,
                sql=sql,
                sub_load=sub_load,
            )
        )
        if self.base_logger:
            self.base_logger.warning(
                message if isinstance(message, str) else json.dumps(message)
            )
        if len(self._pending_logs) > 10:
            self.flush()

    def error(
        self,
        message: str,
        *,
        load: str | None = None,
        sql: str | None = None,
        sub_load: str | None = None,
        error_trackback: str | None = None,
    ):
        self._pending_logs.append(
            LogMessage(
                message=message,
                type="error",
                date=datetime.now(tz=timezone.utc),
                load=load,
                sql=sql,
                sub_load=sub_load,
                error_trackback=error_trackback,
            )
        )
        if self.base_logger:
            self.base_logger.error(
                message if isinstance(message, str) else json.dumps(message)
            )
        if len(self._pending_logs) > 10:
            self.flush()

    def flush(self):
        dummy = LogMessage(
            message="",
            type="",
            date=datetime.now(tz=timezone.utc),
            sql="",
            load="",
            sub_load="",
            error_trackback="",
        )
        self.source.local_pylist_to_delta(
            [p.model_dump() if is_pydantic_2 else p.dict() for p in self._pending_logs],
            self.log_file_path,
            "append",
            dummy_record=dummy.model_dump() if is_pydantic_2 else dummy.dict(),
        )
        self._pending_logs.clear()
