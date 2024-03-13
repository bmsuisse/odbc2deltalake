from pathlib import Path
from typing import Literal, cast
import fsspec
from .azure_utils import convert_options
import adlfs


class FileSystemDestination:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def __truediv__(self, other: str):
        return FileSystemDestination(self.path / other)

    def mkdir(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def get_fs_path(self) -> tuple[fsspec.AbstractFileSystem, str]:
        return (fsspec.filesystem("file"), str(self.path))

    def __str__(self):
        return str(self.path)

    def rm_tree(self):
        if not self.path.exists():
            return
        import shutil

        shutil.rmtree(self.path)

    def exists(self):
        return self.path.exists()

    def upload(self, data: bytes):
        with open(self.path, "wb") as f:
            f.write(data)

    def modified_time(self):
        fs, path = self.get_fs_path()
        return fs.modified(path)

    def remove(self):
        self.path.unlink()

    @property
    def parent(self):
        return FileSystemDestination(self.path.parent)

    def as_path_options(self, flavor: Literal["fsspec", "object_store"]):
        return str(self.path), None

    def as_delta_table(self):
        from deltalake import DeltaTable

        return DeltaTable(self.path)

    def with_suffix(self, suffix: str):
        return FileSystemDestination(self.path.with_suffix(suffix))


class AzureDestination:
    def __init__(self, container: str, path: str, storage_options: dict):
        self.container = container
        self.path = path
        self.storage_options = storage_options

    def to_az_path(self):
        return f"az://{self.container}/{self.path}"

    def mkdir(self):
        pass

    def get_fs_path(self) -> tuple[adlfs.AzureBlobFileSystem, str]:
        opts = cast(dict[str, str], convert_options(self.storage_options, "fsspec"))
        return (fsspec.filesystem("az", **opts), self.to_az_path())

    def upload(self, data: bytes):
        opts = cast(dict[str, str], convert_options(self.storage_options, "fsspec"))

        fs: adlfs.AzureBlobFileSystem = fsspec.filesystem("az", **opts)
        with fs.open(self.to_az_path(), "wb") as f:
            f.write(data)  # type: ignore

    def modified_time(self):
        fs, path = self.get_fs_path()
        return fs.modified(path)

    def __truediv__(self, other: str):
        return AzureDestination(
            self.container,
            self.path.removesuffix("/") + "/" + other,
            self.storage_options,
        )

    @property
    def parent(self):
        return AzureDestination(
            self.container,
            "/".join(self.path.removesuffix("/").split("/")[:-1]),
            self.storage_options,
        )

    def exists(self):
        fs, path = self.get_fs_path()
        return fs.exists(path)

    def rm_tree(self):
        fs, path = self.get_fs_path()
        fs.rm(path, recursive=True)

    def remove(self):
        fs, path = self.get_fs_path()
        fs.rm(path)

    def as_path_options(self, flavor: Literal["fsspec", "object_store"]):
        return self.to_az_path(), cast(
            dict[str, str], convert_options(self.storage_options, flavor)
        )

    def as_delta_table(self):
        from deltalake import DeltaTable

        return DeltaTable(
            self.to_az_path(),
            storage_options=convert_options(self.storage_options, "object_store"),  # type: ignore
        )

    def __str__(self):
        return self.to_az_path()

    def with_suffix(self, suffix: str):
        return AzureDestination(
            self.container, self.path + suffix, self.storage_options
        )


Destination = FileSystemDestination | AzureDestination
