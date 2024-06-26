from odbc2deltalake.destination.destination import Destination
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal, Mapping, Protocol, Any, Sequence, Union
from sqlglot.expressions import Query

if TYPE_CHECKING:
    from odbc2deltalake.metadata import InformationSchemaColInfo

logger = logging.getLogger(__name__)


class ColInfo(Protocol):
    name: str
    nullable: bool


class DeltaOps(Protocol):
    def version(
        self,
    ) -> int: ...

    def vacuum(self, retention_hours: Union[int, None] = None) -> Any: ...

    def restore(self, target: int) -> Any: ...

    def set_properties(self, props: dict[str, str]): ...

    def get_property(self, key: str) -> Union[str, None]: ...

    def column_infos(self) -> Sequence[ColInfo]: ...

    def set_nullable(self, cols: Mapping[str, bool]): ...


class DataSourceReader(ABC):
    @property
    @abstractmethod
    def supports_proc_exec(self) -> bool:
        pass

    @property
    @abstractmethod
    def query_dialect(self) -> str:
        pass

    @abstractmethod
    def local_delta_table_exists(
        self, delta_path: Destination, extended_check=False
    ) -> bool:
        pass

    @abstractmethod
    def source_write_sql_to_delta(
        self,
        sql: str,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
    ):
        pass

    @abstractmethod
    def source_schema_limit_one(self, sql: Query) -> "list[InformationSchemaColInfo]":
        pass

    @abstractmethod
    def source_sql_to_py(self, sql: Union[str, Query]) -> list[dict]:
        pass

    @abstractmethod
    def local_execute_sql_to_py(self, sql: Query) -> list[dict]:
        pass

    @abstractmethod
    def get_local_delta_ops(self, delta_path: Destination) -> DeltaOps:
        pass

    @abstractmethod
    def local_execute_sql_to_delta(
        self,
        sql: Query,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
    ):
        pass

    @abstractmethod
    def local_pylist_to_delta(
        self,
        pylist: list[dict],
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        dummy_record: Union[dict, None] = None,
    ):
        pass

    @abstractmethod
    def local_register_view(self, sql: Query, view_name: str):
        pass

    @abstractmethod
    def local_register_update_view(
        self,
        delta_path: Destination,
        view_name: str,
        *,
        version: Union[int, None] = None,
    ):
        pass

    @abstractmethod
    def local_upsert_into(
        self,
        local_sql_source: Query,
        target_delta: Destination,
        merge_cols: Sequence[str],
    ):
        pass
