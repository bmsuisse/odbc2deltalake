from typing import TYPE_CHECKING
import pytest
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(17)
@pytest.mark.parametrize("conf_name", config_names)
def test_no_change(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """A delta load of an unchanged source compares MAX(delta column) and COUNT(*) with the
    last load (the delta column converted as the load writes it, e.g. a rowversion as bigint)
    and writes nothing."""
    from odbc2deltalake import DBDeltaPathConfigs
    from odbc2deltalake.load_result import NoLoadResult

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user_no_change")[
        conf_name
    ]
    write_db_to_delta_with_check(reader, ("dbo", "user"), dest)  # full load
    versions = {
        name: reader.get_local_delta_ops(path).version()
        for name, path in [
            ("delta", dest / "delta"),
            ("latest_pk", dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION),
        ]
    }

    _, result = write_db_to_delta_with_check(reader, ("dbo", "user"), dest)
    assert isinstance(result, NoLoadResult)
    assert versions == {
        name: reader.get_local_delta_ops(path).version()
        for name, path in [
            ("delta", dest / "delta"),
            ("latest_pk", dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION),
        ]
    }
