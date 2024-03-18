from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Literal, Self
from deltalake import DeltaTable
import fsspec


class Destination(ABC):
    @abstractmethod
    def mkdir(self):
        pass

    @abstractmethod
    def get_fs_path(self) -> tuple[fsspec.AbstractFileSystem, str]:
        pass

    @abstractmethod
    def __truediv__(self, other: str) -> Self:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def rm_tree(self):
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def upload(self, data: bytes):
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
    def as_delta_table(self) -> DeltaTable:
        pass

    @abstractmethod
    def with_suffix(self, suffix: str) -> Self:
        pass

    @abstractmethod
    def path_rename(self, other: "Self"):
        pass

    @abstractmethod
    def path_copy(self, other: "Self"):
        pass
