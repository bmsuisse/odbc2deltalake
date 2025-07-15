from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.parametrize("conf_name", config_names)
def test_insert_while_load(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import WriteConfig, make_writer

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user8")[
        conf_name
    ]

    config = WriteConfig(primary_keys=["User_-_iD"], delta_col="time stamp")

    w = write_db_to_delta_with_check(
        reader, ("dbo", "user8"), dest, write_config=config
    )

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
                     set identity_insert dbo.user8 on;
                INSERT into dbo.user8(FirstName, LastName, [User - iD], companyid) 
                SELECT 'Francis', 'Peterson', 98, 'c1'
                  """
            )
    writer = make_writer(reader, ("dbo", "user8"), dest, write_config=config)

    orig_info = writer.logger.info

    def info(*args, **kwargs):
        msg = args[0]
        if msg.startswith("Start delta step 2"):
            with connection.new_connection(conf_name) as nc:
                with nc.cursor() as cursor:
                    cursor.execute(
                        """delete from dbo.user8 where "User - iD"=98
                        """
                    )
        orig_info(*args, **kwargs)

    writer.logger.info = info
    writer.execute()
    writer.check_delta_consistency()
    import time

    time.sleep(2)

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_scd2",
            use_delta_ext=conf_name == "spark",
        )

        name_tuples = con.execute(
            'SELECT * from v_user_scd2 where "User_-_iD"=98'
        ).fetchall()
        assert len(name_tuples) == 0
        duckdb_create_view_for_delta(
            con,
            (dest / "delta_load/latest_pk_version").as_delta_table(),
            "v_user_lpk",
            use_delta_ext=conf_name == "spark",
        )

        name_tuples = con.execute(
            'SELECT * from v_user_lpk where "User_-_iD"=98'
        ).fetchall()
        assert len(name_tuples) == 0
