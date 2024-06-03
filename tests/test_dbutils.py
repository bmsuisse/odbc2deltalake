from unittest.mock import MagicMock

from odbc2deltalake.destination.databricks import DatabricksDestination


def _container_path_account(path: str):
    path = path.removeprefix("abfss://")
    container, account_path = path.split("@")
    account = account_path.split("/")[0]
    path = "/".join(account_path.split("/")[1:])
    return container, path, account


def test_dbutils():
    test_paths = [
        "abfss://raw@somesuperstore1.dfs.core.windows.net/",
        "abfss://raw@somesuperstore1.dfs.core.windows.net/asdfwe",
        "abfss://raw@somesuperstore1.dfs.core.windows.net/adsfijo/asdfsdf",
    ]
    for test_path in test_paths:
        dbutils = MagicMock()
        container, path, account = _container_path_account(test_path)
        dest = DatabricksDestination(dbutils, container, path, account)
        assert dest.to_az_path() == test_path
        assert str(dest) == test_path
        assert (dest / "asdf").to_az_path() == test_path.removesuffix("/") + "/asdf"
        assert (dest / "asdf").parent.to_az_path() == test_path
        assert (dest / "asdf" / "bsdf").parent.parent.to_az_path() == test_path
        assert (dest / "asdf/bsdf").parent.parent.to_az_path() == test_path
