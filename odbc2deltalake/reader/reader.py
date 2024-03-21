from pathlib import Path
from odbc2deltalake.destination.destination import Destination
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal
from sqlglot.expressions import Query

logger = logging.getLogger(__name__)


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
    def local_execute_sql_to_delta(
        self, sql: Query, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        pass

    @abstractmethod
    def local_register_view(self, sql: Query, view_name: str):
        pass

    @abstractmethod
    def local_register_update_view(self, delta_path: Destination, view_name: str):
        pass
