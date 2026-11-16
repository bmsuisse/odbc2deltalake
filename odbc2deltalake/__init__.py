from pathlib import Path
from typing import Union

from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.reader.reader import DataSourceReader
from odbc2deltalake.write_init import (
    DEFAULT_DATA_TYPE_MAP,
    DBDeltaPathConfigs,
    WriteConfig,
    WriteConfigAndInfos,
    make_writer,
)


def write_db_to_delta(
    source: Union[DataSourceReader, str],
    table_or_query: tuple[str, str],
    destination: Union[Destination, Path],
    write_config: Union[WriteConfig, None] = None,
):
    return make_writer(
        source=source,
        table_or_query=table_or_query,
        destination=destination,
        write_config=write_config,
    ).execute()
