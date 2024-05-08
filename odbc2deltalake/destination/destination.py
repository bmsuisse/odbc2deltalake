from abc import ABC, abstractmethod
from datetime import datetime
from typing_extensions import Literal, Self
from typing import TYPE_CHECKING, Optional

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
    def remove(self, recurse: bool = False):
        pass

    @property
    @abstractmethod
    def parent(self) -> Self:
        pass

    @abstractmethod
    def as_path_options(
        self, flavor: Literal["fsspec", "object_store"]
    ) -> tuple[str, Optional[dict[str, str]]]:
        pass

    @abstractmethod
    def as_delta_table(self) -> "DeltaTable":
        pass
