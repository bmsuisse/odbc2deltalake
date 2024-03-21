from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing_extensions import Literal, Self
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deltalake import DeltaTable


class Destination(ABC):

    @abstractmethod
    def mkdir(self):
        pass

    @abstractmethod
    def __truediv__(self, other: str) -> Self:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def upload_str(self, data: str):
        pass

    @abstractmethod
    def modified_time(self) -> datetime:
        pass

    @abstractmethod
    def remove(self):
        pass

    @property
    @abstractmethod
    def parent(self) -> Self:
        pass

    @abstractmethod
    def as_path_options(
        self, flavor: Literal["fsspec", "object_store"]
    ) -> tuple[str, dict[str, str] | None]:
        pass

    @abstractmethod
    def as_delta_table(self) -> "DeltaTable":
        pass

    @abstractmethod
    def with_suffix(self, suffix: str) -> Self:
        pass
