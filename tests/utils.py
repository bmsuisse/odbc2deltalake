from pathlib import Path
from odbc2deltalake import WriteConfigAndInfos
from odbc2deltalake.write_utils.restore_pk import create_last_pk_version_view
from odbc2deltalake import DBDeltaPathConfigs
from typing import TYPE_CHECKING, Union
import pandas as pd
from sqlglot import from_
import sqlglot.expressions as ex
from odbc2deltalake import make_writer, DataSourceReader, WriteConfig, Destination
from odbc2deltalake.consistency import check_latest_pk
import os

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def _ntz(df_in: pd.DataFrame):
    df = df_in.copy()
    col_times = [
        col for col in df.columns if any([isinstance(x, pd.Timestamp) for x in df[col]])
    ]
    for col in col_times:
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.tz_localize(None)
    return df


def check_latest_pk_pandas(infos: WriteConfigAndInfos):
    lpk_path = infos.destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
    lpk_df = lpk_path.as_delta_table()
    sort_cols = [infos.write_config.get_target_name(pk) for pk in infos.pk_cols]

    def _sort_pd(df: pd.DataFrame):
        if df.shape[0] == 0:
            return df
        return df.sort_values(sort_cols).reset_index(drop=True)

    lpk_pd = _sort_pd(lpk_df.to_pandas())
    _, view_name, success = create_last_pk_version_view(infos, view_prefix="v_tester_")
    assert success
    assert view_name is not None
    latest_pd = _sort_pd(
        pd.DataFrame(infos.source.local_execute_sql_to_py(from_(view_name).select("*")))
    )
    if latest_pd.shape[0] == 0 and lpk_pd.shape[0] == 0:
        return
    latest_pd = _ntz(latest_pd)
    comp = _ntz(lpk_pd).compare(latest_pd)
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
    r = w.execute()
    w.source.local_register_update_view(w.destination / "delta", "last_delta_view")

    check_latest_pk(w)
    check_latest_pk_pandas(w)
    return w, r


if os.getenv("ODBCLAKE_TEST_CONFIGURATION"):
    config_names = [os.getenv("ODBCLAKE_TEST_CONFIGURATION")]
else:
    config_names = (
        ["azure", "spark", "local"]
        if not os.getenv("NO_SPARK", "0") == "1"
        else ["azure", "local"]
    )


def get_test_run_configs(
    connection: "DB_Connection", spark_session: "SparkSession", tbl_dest_name: str
) -> dict[str, tuple[DataSourceReader, Destination]]:
    from pathlib import Path
    from odbc2deltalake.destination.file_system import FileSystemDestination

    sub_path = "/".join(tbl_dest_name.split("/")[0:-1])
    os.makedirs("tests/_db/_azure/" + sub_path, exist_ok=True)
    os.makedirs("tests/_db/_local/" + sub_path, exist_ok=True)
    cfg: dict[str, tuple[DataSourceReader, Destination]] = {}

    if os.getenv("ODBCLAKE_TEST_CONFIGURATION", "azure").lower() == "azure":
        from odbc2deltalake.destination.azure import AzureDestination
        from odbc2deltalake.reader.odbc_reader import ODBCReader

        cfg["azure"] = (
            ODBCReader(
                connection.conn_str["azure"], f"tests/_db/_azure/{tbl_dest_name}.duckdb"
            ),
            AzureDestination("testlakeodbc", tbl_dest_name, {"use_emulator": "true"}),
        )
    if os.getenv("ODBCLAKE_TEST_CONFIGURATION", "local").lower() == "local":
        from odbc2deltalake.reader.odbc_reader import ODBCReader

        cfg["local"] = (
            ODBCReader(
                connection.conn_str["local"], f"tests/_db/_local/{tbl_dest_name}.duckdb"
            ),
            FileSystemDestination(Path(f"tests/_data/{tbl_dest_name}")),
        )
    if spark_session is not None:
        from odbc2deltalake.reader.spark_reader import SparkReader

        cfg["spark"] = (
            SparkReader(spark_session, connection.get_jdbc_options("spark"), jdbc=True),
            FileSystemDestination(
                Path(f"tests/_data/spark/{tbl_dest_name}").absolute()
            ),
        )
    return cfg
