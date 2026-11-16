from datetime import datetime
from typing import Optional, Protocol, Union

from pydantic import BaseModel


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
    def log(self, msg: LogMessage) -> None: ...

    def flush(self) -> None: ...
