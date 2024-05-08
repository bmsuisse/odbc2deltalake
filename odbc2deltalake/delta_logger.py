from datetime import datetime, timezone
from odbc2deltalake.destination.destination import Destination
from pydantic import BaseModel
import pydantic
import logging
from uuid import uuid4
from typing import Union

from odbc2deltalake.reader.reader import DataSourceReader

is_pydantic_2 = int(pydantic.__version__.split(".")[0]) > 1


class LogMessage(BaseModel):
    message: str
    type: str
    date: datetime
    sql: Union[str, None] = None
    load: Union[str, None] = None
    sub_load: Union[str, None] = None
    error_trackback: Union[str, None] = None


class DeltaLogger:
    _pending_logs: list[LogMessage] = []

    def __init__(
        self,
        log_file_path: Destination,
        source: DataSourceReader,
        base_logger: Union[logging.Logger, None] = None,
        print_to_console: bool = False,
    ):
        self.log_file_path = log_file_path
        self.base_logger = base_logger
        self.source = source
        self.id = uuid4()
        self.print_to_console = print_to_console

    def _log(self, msg: LogMessage):
        self._pending_logs.append(msg)
        if self.base_logger or self.print_to_console:
            msg_str = msg.message
            if msg.sql:
                msg_str += f" | SQL: {msg.sql}"
            if msg.error_trackback:
                msg_str += f" | Error: {msg.error_trackback}"
            if msg.load:
                msg_str += (
                    f" | Load: {msg.load}{', ' + msg.sub_load if msg.sub_load else '' }"
                )

            if self.base_logger and msg.type == "info":
                self.base_logger.info(msg_str)
            elif self.base_logger and (msg.type == "warn" or msg.type == "warning"):
                self.base_logger.warning(msg_str)
            elif self.base_logger and msg.type == "error":
                self.base_logger.error(msg_str)

            if self.print_to_console:
                print(msg.type + ": " + msg_str)

        if len(self._pending_logs) > 10:
            self.flush()

    def info(
        self,
        message: str,
        *,
        load: Union[str, None] = None,
        sql: Union[str, None] = None,
        sub_load: Union[str, None] = None,
    ):
        self._log(
            LogMessage(
                message=message,
                type="info",
                date=datetime.now(tz=timezone.utc),
                load=load,
                sql=sql,
                sub_load=sub_load,
            )
        )

    def warning(
        self,
        message: str,
        *,
        load: Union[str, None] = None,
        sql: Union[str, None] = None,
        sub_load: Union[str, None] = None,
        error_trackback: Union[str, None] = None,
    ):
        self._log(
            LogMessage(
                message=message,
                type="warn",
                date=datetime.now(tz=timezone.utc),
                load=load,
                sql=sql,
                sub_load=sub_load,
                error_trackback=error_trackback,
            )
        )

    def error(
        self,
        message: str,
        *,
        load: Union[str, None] = None,
        sql: Union[str, None] = None,
        sub_load: Union[str, None] = None,
        error_trackback: Union[str, None] = None,
    ):
        self._log(
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

    def _append_fields(self, log: dict) -> dict:
        log["logger_id"] = str(self.id)
        return log

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
            [
                self._append_fields(p.model_dump() if is_pydantic_2 else p.dict())
                for p in self._pending_logs
            ],
            self.log_file_path,
            "append",
            dummy_record=dummy.model_dump() if is_pydantic_2 else dummy.dict(),
        )
        self._pending_logs.clear()
