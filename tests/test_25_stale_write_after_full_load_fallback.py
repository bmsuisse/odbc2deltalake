"""_handle_additional_updates (db_to_delta.py) can fall back to a full load
mid-delta-load, when the "additional updates" view can't be built and
restore_last_pk also fails:

    if not restore_success:
        logger.warning("No primary keys found, do a full load")
        do_full_load(infos=infos, mode="append")
        return

That `return` only exits _handle_additional_updates - not do_delta_load.
do_full_load already correctly (re)writes delta_load/latest_pk_version
from the fresh full-load snapshot. But do_delta_load's caller keeps going
right after _handle_additional_updates returns:

    new_delta_load_value = _handle_additional_updates(infos=infos, old_pk_version=old_pk_version)
    ...
    do_deletes(..., old_pk_version=old_pk_version, ...)
    ...
    write_latest_pk(...)   # mode="overwrite" - overwrites latest_pk_version again

do_deletes and write_latest_pk both operate on delta_1/delta_2/
primary_keys_ts, all computed BEFORE the full-load fallback happened (ie.
against pre-restore, stale data), and old_pk_version, which points at the
LATEST_PK_VERSION *before this whole do_delta_load call started* - not the
one do_full_load just correctly wrote. write_latest_pk then overwrites
the correct, freshly-written latest_pk_version with one built from that
stale data.

This test forces that exact path (mocking the "additional_updates" view
creation to fail, and restore_last_pk to fail) against a real postgres
table, and checks whether the final latest_pk_version reflects reality -
via write_db_to_delta_with_check's built-in check_latest_pk /
check_latest_pk_pandas ground-truth comparisons.
"""
from typing import TYPE_CHECKING
from unittest.mock import patch
import pytest
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(25)
@pytest.mark.parametrize("conf_name", config_names)
def test_stale_write_after_full_load_fallback(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    reader, dest = get_test_run_configs(
        connection, spark_session, "dbo/company3_stale_fallback"
    )[conf_name]

    # establish a steady state: full load, then one normal successful delta load
    write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                "insert into dbo.company3(id, name) values "
                "('c_stale1', 'stale fallback test 1')"
            )
    write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)

    # give the next delta load something real to do
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                "insert into dbo.company3(id, name) values "
                "('c_stale2', 'stale fallback test 2')"
            )

    real_register_view = reader.local_register_view

    def _fail_additional_updates(sql, view_name, *a, **kw):
        if view_name == "additional_updates":
            # simulate a concurrent write landing on the source exactly while
            # the "additional updates" detection is failing/being restored -
            # the pre-failure delta_1/primary_keys_ts snapshot was already
            # computed and does NOT include this row; only a fresh read
            # (what the full-load fallback does) will see it.
            with connection.new_connection(conf_name) as nc2:
                with nc2.cursor() as cursor2:
                    cursor2.execute(
                        "insert into dbo.company3(id, name) values "
                        "('c_stale3', 'concurrent during fallback')"
                    )
            raise RuntimeError("simulated: cannot build additional_updates view")
        return real_register_view(sql, view_name, *a, **kw)

    with patch.object(
        reader, "local_register_view", side_effect=_fail_additional_updates
    ), patch(
        "odbc2deltalake.write_utils.restore_pk.restore_last_pk", return_value=False
    ):
        # must not corrupt state even though it takes the full-load-fallback path
        write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
