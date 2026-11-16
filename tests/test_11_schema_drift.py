import dataclasses
from typing import TYPE_CHECKING

import duckdb
import pytest
import sqlglot as sg

from tests.utils import (
    config_names,
    create_delta_view,
    get_test_run_configs,
    write_db_to_delta_with_check,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from tests.conftest import DBConnection


@pytest.mark.parametrize("conf_name", config_names)
def test_schema_drift(connection: "DBConnection", spark_session: "SparkSession", conf_name: str):
    import sqlglot.expressions as ex

    from odbc2deltalake import WriteConfig

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user7")[conf_name]

    config = WriteConfig(
        primary_keys=["User_-_iD"],
        delta_col="time stamp" if reader.source_dialect != "postgres" else "xmin",
        dialect=reader.source_dialect,
        allow_schema_drift=True,
    )
    write_db_to_delta_with_check(reader, ("dbo", "user7"), dest, write_config=config)

    with connection.new_connection(conf_name) as nc, nc.cursor() as cursor:
        cursor.execute(
            """ALTER TABLE dbo.user7 add some_date date not null default('2000-01-01');
                  """
        )

    import time

    time.sleep(2)

    _, r = write_db_to_delta_with_check(reader, ("dbo", "user7"), dest, write_config=config)
    assert r.executed_type == "full"

    with duckdb.connect() as con:
        create_delta_view(
            con,
            dest / "delta",
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
    with connection.new_connection(conf_name) as nc, nc.cursor() as cursor:
        cursor.execute(
            sg.parse_one(
                """ALTER TABLE dbo.user7 alter column Age decimal(20, 3)
                """,
                dialect="tsql",
            ).sql(reader.source_dialect)
        )

    # we assume we can safely insert double into decimal
    _, r = write_db_to_delta_with_check(reader, ("dbo", "user7"), dest, write_config=config)
    assert r.executed_type == "delta"
    fields = reader.get_local_delta_ops(dest / "delta").column_infos()
    age_field = next(f for f in fields if f.column_name.lower() == "age")
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
    c2 = dataclasses.replace(config, load_mode="force_full")
    try:
        write_db_to_delta_with_check(reader, ("dbo", "user7"), dest, write_config=c2)
    except Exception:  # the exception type depends on the configuration (delta-rs / spark) and database
        pass
    else:
        pytest.fail("Expected the force_full load to fail after the incompatible type change")
