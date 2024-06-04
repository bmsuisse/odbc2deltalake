from datetime import datetime, timezone
from odbc2deltalake.destination.destination import Destination
from pydantic import BaseModel
import pydantic
import logging
from uuid import uuid4
from typing import Literal, Optional, Union, Protocol


class LogMessage(BaseModel):
    message: str
    type: str
    date: datetime
    logger_id: str
    logger_name: Optional[str]
    sql: Union[str, None] = None
    load: Union[str, None] = None
    sub_load: Union[str, None] = None
    error_trackback: Union[str, None] = None


class StorageBackend(Protocol):
    def log(self, msg: LogMessage): ...

    def flush(self): ...
