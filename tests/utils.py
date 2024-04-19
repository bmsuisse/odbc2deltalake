from pathlib import Path
from deltalake import DeltaTable
from odbc2deltalake import WriteConfigAndInfos
from odbc2deltalake.destination import Destination
from odbc2deltalake.write_utils.restore_pk import create_last_pk_version_view
from odbc2deltalake.reader.odbc_reader import ODBCReader
from odbc2deltalake import DBDeltaPathConfigs
from typing import TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


def check_latest_pk(infos: WriteConfigAndInfos):
    lpk_path = infos.destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
    lpk_df = lpk_path.as_delta_table()
    lpk_pd = lpk_df.to_pandas()

    _, view_name, success = create_last_pk_version_view(infos)
    assert success
    latest_pd = pd.DataFrame(infos.source.local_execute_sql_to_py(f"select * form {v}"))
    comp = lpk_df.compare(latest_pd)
    if comp.shape[0] > 0:
        print(comp)
    assert comp.shape[0] == 0
