from .destination import Destination
from typing import Literal, cast, TYPE_CHECKING

if TYPE_CHECKING:
    import adlfs


class AzureDestination(Destination):
    def __init__(self, container: str, path: str, storage_options: dict):
        from .azure_utils import convert_options

        self.path = path

        self.container = container
        self.storage_options = storage_options
        import adlfs
        import fsspec

        opts = cast(dict[str, str], convert_options(self.storage_options, "fsspec"))
        self.fs = cast(adlfs.AzureBlobFileSystem, fsspec.filesystem("az", **opts))

    def __truediv__(self, other: str):
        return AzureDestination(
            self.container,
            self.path.removesuffix("/") + "/" + other,
            self.storage_options,
        )

    def to_az_path(self):
        return f"az://{self.container}/{self.path}"

    def mkdir(self):
        pass

    def get_fs_path(self) -> "tuple[adlfs.AzureBlobFileSystem, str]":
        return (self.fs, self.to_az_path())

    def upload_str(self, data: str):
        with self.fs.open(self.to_az_path(), "w", encoding="utf-8") as f:
            f.write(data)  # type: ignore

    def modified_time(self):
        fs, path = self.get_fs_path()
        return fs.modified(path)

    def remove(self, recurse: bool = False):
        fs, path = self.get_fs_path()
        fs.rm(path, recursive=recurse)

    def as_path_options(self, flavor: Literal["fsspec", "object_store"]):
        from .azure_utils import convert_options

        return self.to_az_path(), cast(
            dict[str, str], convert_options(self.storage_options, flavor)
        )

    def as_delta_table(self):
        from .azure_utils import convert_options
        from deltalake import DeltaTable

        return DeltaTable(
            self.to_az_path(),
            storage_options=convert_options(self.storage_options, "object_store"),  # type: ignore
        )

    def __str__(self):
        return self.to_az_path()

    @property
    def parent(self):
        return self.__class__(
            self.container,
            "/".join(self.path.removesuffix("/").split("/")[:-1]),
            self.storage_options,
        )

    def exists(self):
        fs, path = self.get_fs_path()
        return cast(bool, fs.exists(path))
