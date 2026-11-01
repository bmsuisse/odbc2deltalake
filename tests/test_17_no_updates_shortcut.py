import time
from typing import TYPE_CHECKING
import pytest
from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def _pk_ts_version(reader, dest) -> int:
    return reader.get_local_delta_ops(dest / "delta_load" / "primary_keys_ts").version()


@pytest.mark.order(22)
@pytest.mark.parametrize("conf_name", config_names)
def test_unchanged_load_takes_shortcut(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake.load_result import NoLoadResult

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/rv_shortcut")[
        conf_name
    ]
    is_pg = reader.source_dialect == "postgres"
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS dbo.rv_shortcut")
        with nc.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE dbo.rv_shortcut (id int primary key, val varchar(50))"
                if is_pg
                else "CREATE TABLE dbo.rv_shortcut (id int primary key, val varchar(50), rv rowversion)"
            )
        with nc.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dbo.rv_shortcut (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
            )

    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])

    time.sleep(1)
    version = _pk_ts_version(reader, dest) if _has_pk_ts(dest) else None
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert isinstance(res, NoLoadResult), "unchanged load must take the shortcut"
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])
    if version is not None:
        assert _pk_ts_version(reader, dest) == version, "PK/TS must not be re-read"

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("UPDATE dbo.rv_shortcut SET val = 'changed' WHERE id = 2")
    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert not isinstance(res, NoLoadResult)
    assert _delta_stats_for(dest, conf_name) == (5, 4, [2])

    # a delete without any other change: count differs, so the shortcut must not skip it
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("DELETE FROM dbo.rv_shortcut WHERE id = 3")
    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert not isinstance(res, NoLoadResult)
    assert _delta_stats_for(dest, conf_name) == (6, 4, [2, 3])
    assert _deleted_ids(dest, conf_name) == [3]

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert isinstance(res, NoLoadResult)


def _has_pk_ts(dest) -> bool:
    return (dest / "delta_load" / "primary_keys_ts").exists()


def _view(con, dest, conf_name):
    from deltalake2db import duckdb_create_view_for_delta

    duckdb_create_view_for_delta(
        con,
        (dest / "delta").as_delta_table(),
        "v_sc",
        use_delta_ext=conf_name == "spark",
    )


def _delta_stats_for(dest, conf_name: str):
    import duckdb

    with duckdb.connect() as con:
        _view(con, dest, conf_name)
        total = con.execute("SELECT count(*) FROM v_sc").fetchall()[0][0]
        full = con.execute(
            'SELECT count(*) FROM v_sc WHERE "__is_full_load"'
        ).fetchall()[0][0]
        changed = [
            r[0]
            for r in con.execute(
                'SELECT id FROM v_sc WHERE NOT "__is_full_load" ORDER BY id'
            ).fetchall()
        ]
        return total, full, changed


def _deleted_ids(dest, conf_name: str):
    import duckdb

    with duckdb.connect() as con:
        _view(con, dest, conf_name)
        return [
            r[0]
            for r in con.execute(
                'SELECT id FROM v_sc WHERE "__is_deleted" ORDER BY id'
            ).fetchall()
        ]
