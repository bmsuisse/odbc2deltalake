"""A connection drop in delta step 2 (after primary_keys_ts was rewritten) must not make the next run skip the load."""

import dataclasses
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
import sqlglot as sg

from odbc2deltalake import WriteConfig
from odbc2deltalake.db_to_delta import _load_updates_to_delta
from odbc2deltalake.load_result import NoLoadResult
from tests.utils import config_names, get_test_run_configs, write_db_to_delta_with_check

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from tests.conftest import DBConnection


def _fail_once() -> Any:
    calls: list[int] = []

    def _wrapper(*a: Any, **kw: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("simulated connection drop")
        return _load_updates_to_delta(*a, **kw)

    return _wrapper, calls


def _has_row(reader: Any, dest: Any, id_: str) -> bool:
    reader.local_register_update_view(dest / "delta", "retry_check_view")
    return any(r["id"] == id_ for r in reader.local_execute_sql_to_py(sg.parse_one("select id from retry_check_view")))


def _prepare(connection: "DBConnection", spark_session: "SparkSession", conf_name: str, name: str, row_id: str) -> Any:
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{name}")[conf_name]
    write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
    with connection.new_connection(conf_name) as nc, nc.cursor() as cursor:
        cursor.execute(f"insert into dbo.company3(id, name) values ('{row_id}', 'retry test')")
    return reader, dest


@pytest.mark.order(26)
@pytest.mark.parametrize("conf_name", config_names)
def test_rerun_after_failed_step2_loads_rows(
    connection: "DBConnection", spark_session: "SparkSession", conf_name: str
) -> None:
    reader, dest = _prepare(connection, spark_session, conf_name, "company3_rerun", "c_rerun1")
    fail, _ = _fail_once()
    with (
        patch("odbc2deltalake.db_to_delta._load_updates_to_delta", side_effect=fail),
        pytest.raises(ConnectionError),
    ):
        write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
    # a plain re-run (eg a notebook-level retry) must not say "No updates"
    _, res = write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
    assert not isinstance(res, NoLoadResult)
    assert _has_row(reader, dest, "c_rerun1")


@pytest.mark.order(26)
@pytest.mark.parametrize("conf_name", config_names)
def test_retry_option_recovers(connection: "DBConnection", spark_session: "SparkSession", conf_name: str) -> None:
    reader, dest = _prepare(connection, spark_session, conf_name, "company3_retry", "c_retry1")
    fail, calls = _fail_once()
    cfg = dataclasses.replace(WriteConfig(dialect=reader.source_dialect), retry_attempts=2, retry_wait_seconds=0)
    with patch("odbc2deltalake.db_to_delta._load_updates_to_delta", side_effect=fail):
        _, res = write_db_to_delta_with_check(reader, ("dbo", "company3"), dest, cfg)
    assert len(calls) == 2
    assert not isinstance(res, NoLoadResult)
    assert _has_row(reader, dest, "c_retry1")


@pytest.mark.order(26)
@pytest.mark.parametrize("conf_name", config_names)
def test_rerun_after_killed_run_loads_rows(
    connection: "DBConnection", spark_session: "SparkSession", conf_name: str
) -> None:
    reader, dest = _prepare(connection, spark_session, conf_name, "company3_killed", "c_killed1")
    fail, _ = _fail_once()
    # a kill skips the cleanup in exec_write_db_to_delta: stale primary_keys_ts and lock.txt stay behind
    with (
        patch("odbc2deltalake.db_to_delta._load_updates_to_delta", side_effect=fail),
        patch.object(type(dest / "x"), "remove", lambda self, recurse=False: None),
        pytest.raises(ConnectionError),
    ):
        write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
    assert (dest / "meta/lock.txt").exists()
    assert (dest / "delta_load/primary_keys_ts").exists()
    _, res = write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
    assert not isinstance(res, NoLoadResult)
    assert _has_row(reader, dest, "c_killed1")


@pytest.mark.order(26)
def test_adbc_reader_reconnects_on_retry(connection: "DBConnection") -> None:
    if connection.source_server != "postgres":
        pytest.skip("ADBC/postgres only")  # ty: ignore[too-many-positional-arguments]

    import adbc_driver_postgresql.dbapi as adbc_pg

    from odbc2deltalake.reader.adbc_reader import ADBCReader

    connstr = next(iter(connection.conn_str.values()))
    reader = ADBCReader(lambda: adbc_pg.connect(connstr, autocommit=True), source_dialect="postgres")
    reader.connection.close()  # the drop
    with pytest.raises(Exception):  # noqa: B017
        reader.source_sql_to_py("select 1 as x")
    reader.reset_connection()
    assert reader.source_sql_to_py("select 1 as x")[0]["x"] == 1
