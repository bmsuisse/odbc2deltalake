from pathlib import Path
from deltalake import DeltaTable
from odbc2deltalake import WriteConfigAndInfos
from odbc2deltalake.destination import Destination
from odbc2deltalake.write_utils.restore_pk import create_last_pk_version_view
from odbc2deltalake.reader.odbc_reader import ODBCReader
from odbc2deltalake import DBDeltaPathConfigs
from typing import TYPE_CHECKING
import pandas as pd
from sqlglot import from_
import sqlglot.expressions as ex
from odbc2deltalake import make_writer, DataSourceReader, WriteConfig, Destination

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


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
    source: DataSourceReader | str,
    table_or_query: tuple[str, str] | ex.Query,
    destination: Destination | Path,
    write_config: WriteConfig | None = None,
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
