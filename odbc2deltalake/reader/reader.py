from pathlib import Path
from odbc2deltalake.destination.destination import Destination
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal, Protocol, Any
from sqlglot.expressions import Query

logger = logging.getLogger(__name__)


class DeltaOps(Protocol):
    def version(
        self,
    ) -> int: ...

    def vacuum(self, retention_hours: int | None = None) -> Any: ...

    def restore(self, target: int) -> Any: ...
class DataSourceReader(ABC):

    @property
    @abstractmethod
    def query_dialect(self) -> str:
        pass

    @abstractmethod
    def source_write_sql_to_delta(
        self, sql: str, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        pass

    @abstractmethod
    def source_sql_to_py(self, sql: str | Query) -> list[dict]:
        pass

    @abstractmethod
    def local_execute_sql_to_py(self, sql: Query) -> list[dict]:
        pass

    @abstractmethod
    def get_local_delta_ops(self, delta_path: Destination) -> DeltaOps:
        pass

    @abstractmethod
    def local_execute_sql_to_delta(
        self, sql: Query, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        pass

    @abstractmethod
    def local_register_view(self, sql: Query, view_name: str):
        pass

    @abstractmethod
    def local_register_update_view(
        self, delta_path: Destination, view_name: str, *, version: int | None = None
    ):
        pass
