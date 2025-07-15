from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs
import dataclasses
import sqlglot as sg

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.parametrize("conf_name", config_names)
def test_schema_drift(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import WriteConfig
    import sqlglot.expressions as ex

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user7")[
        conf_name
    ]

    config = WriteConfig(
        primary_keys=["User_-_iD"],
        delta_col="time stamp" if reader.source_dialect != "postgres" else "xmin",
        dialect=reader.source_dialect,
    )
    w = write_db_to_delta_with_check(
        reader, ("dbo", "user7"), dest, write_config=config
    )

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """ALTER TABLE dbo.user7 add some_date date not null default('2000-01-01');
                  """
            )

    import time

    time.sleep(2)

    _, r = write_db_to_delta_with_check(
        reader, ("dbo", "user7"), dest, write_config=config
    )
    assert r.executed_type == "full"

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_scd2",
            use_delta_ext=conf_name == "spark",
        )
        from datetime import date

        name_tuples = con.execute(
            'SELECT FirstName, LastName, some_date, __is_deleted  from v_user_scd2 order by "User_-_iD", __timestamp'
        ).fetchall()
        assert name_tuples == [
            ("John", "Anders", None, False),
            ("John", "Anders", date(2000, 1, 1), False),
            ("Peter", "Johniingham", None, False),
            ("Peter", "Johniingham", date(2000, 1, 1), False),
            ("Petra", "wayne", None, False),
            ("Petra", "wayne", date(2000, 1, 1), False),
        ]
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                sg.parse_one(
                    """ALTER TABLE dbo.user7 alter column Age float
                """,
                    dialect="tsql",
                ).sql(reader.source_dialect)
            )

    # we assume we can safely insert double into decimal
    _, r = write_db_to_delta_with_check(
        reader, ("dbo", "user7"), dest, write_config=config
    )
    assert r.executed_type == "delta"
    fields = reader.get_local_delta_ops(dest / "delta").column_infos()
    age_field = next((f for f in fields if f.column_name.lower() == "age"))
    assert age_field.data_type.this == ex.DataType.Type.DECIMAL
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """ALTER TABLE dbo.user7 drop column Age;
                """
            )
        with nc.cursor() as cursor:
            cursor.execute(
                """ALTER TABLE dbo.user7 add Age xml not null default('<a></a>');
                """
            )
    with pytest.raises(Exception):
        c2 = dataclasses.replace(config, load_mode="force_full")
        write_db_to_delta_with_check(reader, ("dbo", "user7"), dest, write_config=c2)
