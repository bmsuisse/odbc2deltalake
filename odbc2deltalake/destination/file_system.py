from typing import Literal, TYPE_CHECKING
from .destination import Destination
from pathlib import Path
import shutil

if TYPE_CHECKING:
    import fsspec


class FileSystemDestination(Destination):
    def __init__(self, path: str | Path):
        self.path = Path(path)
        import fsspec

        self.fs = fsspec.filesystem("file")

    def mkdir(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def get_fs_path(self) -> "tuple[fsspec.AbstractFileSystem, str]":
        return (self.fs, str(self.path))

    def __str__(self):
        return str(self.path)

    def rm_tree(self):
        if not self.path.exists():
            return

        shutil.rmtree(self.path)

    def exists(self):
        return self.path.exists()

    def upload_str(self, data: str):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(data)

    def modified_time(self):
        fs, path = self.get_fs_path()
        return fs.modified(path)

    def remove(self):
        self.path.unlink()

    @property
    def parent(self):
        return self.__class__(self.path.parent)

    def as_path_options(self, flavor: Literal["fsspec", "object_store"]):
        return str(self.path), None

    def as_delta_table(self):
        from deltalake import DeltaTable

        return DeltaTable(self.path)

    def with_suffix(self, suffix: str):
        return FileSystemDestination(self.path.with_suffix(suffix))

    def __truediv__(self, other: str):
        return FileSystemDestination(self.path / other)
