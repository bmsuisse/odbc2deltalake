from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb

from odbc2deltalake.reader.spark_reader import SparkReader
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(16)
@pytest.mark.parametrize("conf_name", config_names)
def test_delta_query(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import DBDeltaPathConfigs, WriteConfig

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user6")[
        conf_name
    ]

    config = WriteConfig(primary_keys=["User_-_iD"], delta_col="time stamp")
    w, r = write_db_to_delta_with_check(
        reader, ("dbo", "user6"), dest, write_config=config
    )
    assert r.executed_type == "full"
    dt_or_path = (
        dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
    ).as_delta_table()

    # we're evil and manipulate the latest pk!
    if isinstance(reader, SparkReader) and isinstance(dt_or_path, Path):
        reader.spark.sql(f"delete from delta.`{dt_or_path}` where `User - iD` = 2")
    else:
        assert not isinstance(dt_or_path, Path)
        stats = dt_or_path.delete(' "User_-_iD" = 2')
    assert stats["num_deleted_rows"] == 1

    _, fixed = w.check_delta_consistency(auto_fix=True)
    assert fixed

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """DELETE FROM dbo.[user6] where "User - iD"=2;
                  """
            )
        with nc.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[user6]")
            alls = cursor.fetchall()
            print(alls)

    import time

    time.sleep(2)

    write_db_to_delta_with_check(reader, ("dbo", "user6"), dest, write_config=config)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_scd2",
            use_delta_ext=conf_name == "spark",
        )

        name_tuples = con.execute(
            'SELECT FirstName, LastName, __is_deleted  from v_user_scd2 order by "User_-_iD", __timestamp'
        ).fetchall()
        assert name_tuples == [
            ("John", "Anders", False),
            ("Peter", "Johniingham", False),
            (None, None, True),
            ("Petra", "wayne", False),
        ]
