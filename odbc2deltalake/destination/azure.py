from .destination import Destination
from typing import Literal, cast
import fsspec
import adlfs


class AzureDestination(Destination):
    def __init__(self, container: str, path: str, storage_options: dict):
        from .azure_utils import convert_options

        self.path = path

        self.container = container
        self.storage_options = storage_options

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

    def get_fs_path(self) -> tuple[adlfs.AzureBlobFileSystem, str]:
        return (self.fs, self.to_az_path())

    def upload(self, data: bytes):
        with self.fs.open(self.to_az_path(), "wb") as f:
            f.write(data)  # type: ignore

    def modified_time(self):
        fs, path = self.get_fs_path()
        return fs.modified(path)

    def remove(self):
        fs, path = self.get_fs_path()
        fs.rm(path)

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

    def with_suffix(self, suffix: str):
        return AzureDestination(
            self.container, self.path + suffix, self.storage_options
        )

    def path_rename(self, other: "AzureDestination"):
        from .azure_utils import get_data_lake_client

        with get_data_lake_client(self.storage_options) as client:
            client.get_directory_client(self.container, self.path).rename_directory(
                f"{other.container}/{other.path}"
            )

    def path_copy(self, other: "AzureDestination"):
        from .azure_utils import get_data_lake_client
        from azure.storage.filedatalake import PathProperties

        with get_data_lake_client(self.storage_options) as client:
            with client.get_file_system_client(self.container) as fsc:
                for path in fsc.get_paths(self.path, recursive=True):

                    pp = cast(PathProperties, path)
                    if pp.is_directory:
                        continue
                    name: str = pp.name
                    relative_path = name[len(self.path) :].removeprefix("/")
                    self.fs.cp_file(
                        f"az://{self.container}/{pp.name}",
                        f"az://{other.container}/{other.path}/{relative_path}",
                    )

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

    def rm_tree(self):
        fs, path = self.get_fs_path()
        fs.rm(path, recursive=True)
