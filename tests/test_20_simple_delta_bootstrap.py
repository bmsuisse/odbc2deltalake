"""Reproduces https://github.com/bmsuisse/odbc2deltalake/issues/67

If a delta destination is bootstrapped by a full load that has no delta_col
(e.g. an "init" style load where the delta column couldn't be detected /
wasn't configured yet), `do_full_load` returns early without ever writing
`delta_load/latest_pk_version` (see `db_to_delta.py`, `do_full_load`: "if
infos.delta_col is None: return FullLoadResult()").

A later `simple_delta`/`simple_delta_check` load against that same
destination (now with a proper delta_col configured) skips the bootstrap
guard that the non-simple delta path has (`do_delta_load` sets
`last_pk_path = None` whenever `simple=True`, so the
"Primary keys missing, try to restore" / "do a full load" fallback never
runs). Execution falls through to `write_latest_pk(merge_delta=False)` ->
`_get_latest_pk_query(merge_delta=False)`, which unconditionally tries to
register `delta_load/primary_keys_ts` as a view - a path that was never
created, since `primary_keys_ts` is only written by the non-simple delta
path's `_retrieve_primary_key_data`.
"""

import dataclasses
from typing import TYPE_CHECKING
import pytest
from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(21)
@pytest.mark.parametrize("conf_name", config_names)
def test_simple_delta_after_delta_col_less_init(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import WriteConfig, make_writer
    from odbc2deltalake.db_to_delta import do_full_load

    # use a destination path distinct from other tests targeting dbo.company3,
    # so we control the bootstrap state (no leftover latest_pk_version)
    reader, dest = get_test_run_configs(
        connection, spark_session, "dbo/company3_bootstrap"
    )[conf_name]

    # Step 1: "init" load with no delta_col - mirrors bootstrapping a table
    # before a delta column is known/configured. do_full_load with
    # delta_col=None does not write delta_load/latest_pk_version.
    init_config = WriteConfig(dialect=reader.source_dialect)
    init_infos = make_writer(reader, ("dbo", "company3"), dest, init_config)
    init_infos = dataclasses.replace(init_infos, delta_col=None)
    do_full_load(init_infos, mode="overwrite")

    assert not (dest / "delta_load" / "latest_pk_version").exists()
    assert (dest / "delta").exists()

    # Change the source so there actually is something to sync - otherwise
    # do_delta_load short-circuits on "No updates, done" before ever
    # reaching write_latest_pk, and the bug wouldn't be exercised.
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                "insert into dbo.company3(id, name) values "
                "('c301', 'company3_bootstrap_test') "
                "on conflict do nothing"
                if reader.source_dialect == "postgres"
                else "if not exists (select 1 from dbo.company3 where id = 'c301') "
                "insert into dbo.company3(id, name) values ('c301', 'company3_bootstrap_test')"
            )

    # Step 2: a normal simple_delta load against the same destination, now
    # with a properly detected delta_col. Since destination/delta already
    # exists, exec_write_db_to_delta routes straight into do_delta_load
    # instead of do_full_load, and the simple path has no bootstrap guard.
    simple_config = WriteConfig(
        load_mode="simple_delta", dialect=reader.source_dialect
    )
    simple_infos = make_writer(reader, ("dbo", "company3"), dest, simple_config)
    assert simple_infos.delta_col is not None, "test setup needs a real delta col"

    simple_infos.execute()  # must not raise
