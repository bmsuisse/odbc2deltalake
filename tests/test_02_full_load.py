from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from datetime import date

from odbc2deltalake.query import sql_quote_value
from .utils import config_names, get_test_run_configs


if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(4)
@pytest.mark.parametrize("conf_name", config_names)
def test_first_load_always_full(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import write_db_to_delta

    reader, dest = get_test_run_configs(
        connection, spark_session, "long_schema/long_table_name2"
    )[conf_name]

    write_db_to_delta(
        reader,
        ("long schema", "long table name_as_view"),
        dest,
    )
    import time

    time.sleep(2)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_long_table_name_temp",
            use_delta_ext=conf_name == "spark",
        )
        res = con.execute(
            "select max(__timestamp) from v_long_table_name_temp s"
        ).fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """INSERT INTO [long schema].[long table name] ([long column name], dt, [date])
    SELECT 5,
        '<root><child>text</child></root>',
        '2025-01-01'"""
            )

    write_db_to_delta(
        reader,
        ("long schema", "long table name_as_view"),
        dest,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_long_table_name",
            use_delta_ext=conf_name == "spark",
        )

        name_tuples = con.execute(
            f'SELECT date from v_long_table_name where __timestamp>{sql_quote_value(max_valid_from)} order by "long_column_name"'
        ).fetchall()
        assert name_tuples == [
            (
                date(
                    2023,
                    1,
                    1,
                ),
            ),
            (
                date(
                    2024,
                    1,
                    1,
                ),
            ),
            (
                date(
                    2025,
                    1,
                    1,
                ),
            ),
        ]
