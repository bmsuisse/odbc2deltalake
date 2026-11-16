from datetime import datetime, timezone
from typing import Literal, Optional

from odbc2deltalake.destination.destination import Destination


class DatabricksDestination(Destination):
    def __init__(self, dbutils, container: str, path: str, account: str, scheme: str = "abfss") -> None:
        self.path = path.removeprefix("/")
        if "." not in account:
            account = account + ".dfs.core.windows.net"
        self.account = account
        self.container = container
        self.dbutils = dbutils
        self.scheme = scheme
        self.fs = None

    def __truediv__(self, other: str):
        return DatabricksDestination(
            self.dbutils,
            container=self.container,
            path=self.path.removesuffix("/") + "/" + other,
            account=self.account,
            scheme=self.scheme,
        )

    def to_az_path(self) -> str:
        return f"{self.scheme}://{self.container}@{self.account}/{self.path}"

    def mkdir(self) -> None:
        self.dbutils.fs.mkdirs(self.to_az_path())

    def upload_str(self, data: str) -> None:
        self.dbutils.fs.put(self.to_az_path(), data, overwrite=True)

    def modified_time(self):
        res = self.dbutils.fs.ls(self.to_az_path())
        assert len(res) == 1
        mod_time: int = res[0].modificationTime
        return datetime.fromtimestamp(mod_time / 1000, tz=timezone.utc)

    def remove(self, recurse: bool = False) -> None:
        self.dbutils.fs.rm(self.to_az_path(), recurse=recurse)

    def as_path_options(self, flavor: Literal["fsspec", "object_store"]):
        raise NotImplementedError("not an option for databricks")

    def as_delta_table(self):
        raise NotImplementedError("not an option for databricks")

    def __str__(self) -> str:
        return self.to_az_path()

    @property
    def parent(self):
        new_path = "/".join(self.path.removesuffix("/").split("/")[:-1])

        return DatabricksDestination(
            self.dbutils,
            container=self.container,
            path=new_path,
            account=self.account,
            scheme=self.scheme,
        )

    def exists(self) -> Optional[bool]:
        try:
            self.dbutils.fs.ls(self.to_az_path())
            return True
        except Exception:
            return False
