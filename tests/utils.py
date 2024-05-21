from pathlib import Path
from odbc2deltalake import WriteConfigAndInfos
from odbc2deltalake.write_utils.restore_pk import create_last_pk_version_view
from odbc2deltalake import DBDeltaPathConfigs
from typing import TYPE_CHECKING, Union
import pandas as pd
from sqlglot import from_
import sqlglot.expressions as ex
from odbc2deltalake import make_writer, DataSourceReader, WriteConfig, Destination
import os

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def check_latest_pk(infos: WriteConfigAndInfos):
    lpk_path = infos.destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
    lpk_df = lpk_path.as_delta_table()
    sort_cols = [infos.write_config.get_target_name(pk) for pk in infos.pk_cols]
    lpk_pd = lpk_df.to_pandas().sort_values(sort_cols).reset_index(drop=True)
    _, view_name, success = create_last_pk_version_view(infos, view_prefix="v_tester_")
    assert success
    assert view_name is not None
    latest_pd = (
        pd.DataFrame(infos.source.local_execute_sql_to_py(from_(view_name).select("*")))
        .sort_values(sort_cols)
        .reset_index(drop=True)
    )
    comp = lpk_pd.compare(latest_pd)
    if comp.shape[0] > 0:
        print(comp)
    assert comp.shape[0] == 0


def write_db_to_delta_with_check(
    source: Union[DataSourceReader, str],
    table_or_query: Union[tuple[str, str], ex.Query],
    destination: Union[Destination, Path],
    write_config: Union[WriteConfig, None] = None,
):
    w = make_writer(
        source=source,
        table_or_query=table_or_query,
        destination=destination,
        write_config=write_config,
    )
    w.logger.print_to_console = True
    w.execute()
    w.source.local_register_update_view(w.destination / "delta", "last_delta_view")

    check_latest_pk(w)
    return w


config_names = (
    ["azure", "spark", "local"]
    if not os.getenv("NO_SPARK", "0") == "1"
    else ["azure", "local"]
)


def get_test_run_configs(
    connection: "DB_Connection", spark_session: "SparkSession", tbl_dest_name: str
) -> dict[str, tuple[DataSourceReader, Destination]]:
    from odbc2deltalake.reader.spark_reader import SparkReader
    from odbc2deltalake.destination.azure import AzureDestination
    from pathlib import Path
    from odbc2deltalake.destination.file_system import FileSystemDestination
    from odbc2deltalake.reader.odbc_reader import ODBCReader

    sub_path = "/".join(tbl_dest_name.split("/")[0:-1])
    os.makedirs("tests/_db/_azure/" + sub_path, exist_ok=True)
    os.makedirs("tests/_db/_local/" + sub_path, exist_ok=True)
    cfg = {
        "azure": (
            ODBCReader(
                connection.conn_str["azure"], f"tests/_db/_azure/{tbl_dest_name}.duckdb"
            ),
            AzureDestination("testlakeodbc", tbl_dest_name, {"use_emulator": "true"}),
        ),
        "local": (
            ODBCReader(
                connection.conn_str["local"], f"tests/_db/_local/{tbl_dest_name}.duckdb"
            ),
            FileSystemDestination(Path(f"tests/_data/{tbl_dest_name}")),
        ),
    }
    if spark_session is not None:
        cfg["spark"] = (
            SparkReader(spark_session, connection.get_jdbc_options("spark"), jdbc=True),
            FileSystemDestination(
                Path(f"tests/_data/spark/{tbl_dest_name}").absolute()
            ),
        )
    return cfg
