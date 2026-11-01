import time
from typing import TYPE_CHECKING
import pytest
import duckdb
from deltalake2db import duckdb_create_view_for_delta
from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def _delta_stats(dest, conf_name: str):
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_rv",
            use_delta_ext=conf_name == "spark",
        )
        total = con.execute("SELECT count(*) FROM v_rv").fetchall()[0][0]
        full_loads = con.execute(
            'SELECT count(*) FROM v_rv WHERE "__is_full_load"'
        ).fetchall()[0][0]
        changed_ids = [
            r[0]
            for r in con.execute(
                'SELECT id FROM v_rv WHERE NOT "__is_full_load" ORDER BY id'
            ).fetchall()
        ]
        return total, full_loads, changed_ids


@pytest.mark.order(20)
@pytest.mark.parametrize("conf_name", config_names)
def test_rowversion_change_detection(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """A table with a primary key and a rowversion column must only append changed rows
    on later loads, no matter which reader is used."""
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake.write_init import get_delta_col
    from odbc2deltalake.metadata import get_columns

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/rv_load")[
        conf_name
    ]
    is_pg = reader.source_dialect == "postgres"
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS dbo.rv_load")
        with nc.cursor() as cursor:
            if is_pg:
                cursor.execute(
                    "CREATE TABLE dbo.rv_load (id int primary key, val varchar(50), amount decimal(15, 3))"
                )
            else:
                cursor.execute(
                    "CREATE TABLE dbo.rv_load (id int primary key, val varchar(50), amount decimal(15, 3), rv rowversion)"
                )
        with nc.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dbo.rv_load (id, val, amount) VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', 3.5), (4, 'd', 4.5)"
            )

    cols = get_columns(reader, ("dbo", "rv_load"), dialect=reader.source_dialect)
    delta_col = get_delta_col(cols, reader.source_dialect, is_physical_table=True)
    assert delta_col is not None, "rowversion/xmin must be detected as delta column"
    if not is_pg:
        assert delta_col.column_name == "rv"
        assert delta_col.data_type_str.lower() in ["timestamp", "rowversion"]

    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "rv_load"), dest, cfg)
    total, full_loads, changed = _delta_stats(dest, conf_name)
    assert (total, full_loads, changed) == (4, 4, [])

    time.sleep(1)
    write_db_to_delta(reader, ("dbo", "rv_load"), dest, cfg)  # nothing changed
    assert _delta_stats(dest, conf_name) == (4, 4, [])

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("UPDATE dbo.rv_load SET val = 'changed' WHERE id = 2")
    time.sleep(1)
    write_db_to_delta(reader, ("dbo", "rv_load"), dest, cfg)
    assert _delta_stats(dest, conf_name) == (5, 4, [2])

    time.sleep(1)
    write_db_to_delta(reader, ("dbo", "rv_load"), dest, cfg)  # nothing changed again
    assert _delta_stats(dest, conf_name) == (5, 4, [2])
