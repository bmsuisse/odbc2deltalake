from datetime import datetime
from pydantic import BaseModel
from typing import Optional, Union, Protocol


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
