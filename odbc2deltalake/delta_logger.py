from datetime import datetime, timezone
from odbc2deltalake.destination.destination import Destination
import pydantic
import logging
from uuid import uuid4
from typing import Literal, Optional, Union
from .logging import LogMessage, StorageBackend
from odbc2deltalake.reader.reader import DataSourceReader

is_pydantic_2 = int(pydantic.__version__.split(".")[0]) > 1


class DeltaStorageBackend(StorageBackend):
    _pending_logs: list[LogMessage] = []

    def __init__(self, log_file_path: Destination, source: DataSourceReader):
        self.log_file_path = log_file_path
        self.source = source

    def log(self, msg: LogMessage):
        self._pending_logs.append(msg)
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
            logger_id="id",
            logger_name="dummy",
        )
        self.source.local_pylist_to_delta(
            [p.model_dump() if is_pydantic_2 else p.dict() for p in self._pending_logs],
            self.log_file_path,
            "append",
            dummy_record=dummy.model_dump() if is_pydantic_2 else dummy.dict(),
        )
        self._pending_logs.clear()


class DeltaLogger:
    def __init__(
        self,
        log_file_path: Destination,
        source: DataSourceReader,
        base_logger: Union[logging.Logger, None] = None,
        print_to_console: bool = False,
        log_name: Optional[str] = None,
        storage_backend: Optional[
            Union[StorageBackend, Literal["default"]]
        ] = "default",
    ):
        self.log_file_path = log_file_path
        self.base_logger = base_logger
        self.source = source
        self.id = uuid4()
        self.print_to_console = print_to_console
        self.log_name = log_name
        if storage_backend == "default":
            self.storage_backend = DeltaStorageBackend(log_file_path, source)
        else:
            self.storage_backend = storage_backend

    def _log(self, msg: LogMessage):
        if self.storage_backend:
            self.storage_backend.log(msg)
        if self.base_logger or self.print_to_console:
            msg_str = msg.message
            if msg.sql:
                msg_str += f" | SQL: {msg.sql}"
            if msg.error_trackback:
                msg_str += f" | Error: {msg.error_trackback}"
            if msg.load:
                msg_str += (
                    f" | Load: {msg.load}{', ' + msg.sub_load if msg.sub_load else ''}"
                )

            if self.base_logger and msg.type == "info":
                self.base_logger.info(msg_str)
            elif self.base_logger and (msg.type == "warn" or msg.type == "warning"):
                self.base_logger.warning(msg_str)
            elif self.base_logger and msg.type == "error":
                self.base_logger.error(msg_str)

            if self.print_to_console:
                print(msg.type + ": " + msg_str)

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
                logger_id=str(self.id),
                logger_name=self.log_name,
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
                logger_id=str(self.id),
                logger_name=self.log_name,
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
                logger_id=str(self.id),
                logger_name=self.log_name,
            )
        )

    def flush(self):
        if self.storage_backend:
            self.storage_backend.flush()
