"""_get_table_cols' non-tsql (postgres) column query compared
`ccu.is_identity` against lowercase 'yes'. Postgres's real
information_schema.columns.is_identity - like every other yes/no column in
information_schema (is_nullable, is_updatable, etc.) - uses uppercase
'YES'/'NO' per the SQL standard, confirmed directly against a live postgres
container:

    select is_identity from information_schema.columns
    where table_name='<a table with a GENERATED ALWAYS AS IDENTITY column>'
    -> 'YES'

So `is_identity=='yes'` never matched, and every postgres column - including
genuine identity columns - was always reported as is_identity=False. The
only consumer (db_to_delta.py's do_delta_load/exec_write_db_to_delta) uses
this to auto-select an identity primary key as the delta column for
append_inserts mode when no delta_col is configured; that auto-detection
silently never fired for postgres.
"""
from typing import TYPE_CHECKING
import pytest

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(24)
def test_postgres_identity_column_detected(connection: "DB_Connection"):
    if connection.source_server != "postgres":
        pytest.skip("is_identity casing bug is specific to the postgres query")

    from odbc2deltalake.metadata import _get_table_cols
    from odbc2deltalake.reader.adbc_reader import ADBCReader
    import adbc_driver_postgresql.dbapi as adbc_pg

    connstr = connection.conn_str["local"]
    conn = adbc_pg.connect(connstr, autocommit=True)
    try:
        with conn.cursor() as c:
            c.execute("drop table if exists dbo.identity_probe")
            c.execute(
                "create table dbo.identity_probe ("
                "id integer generated always as identity primary key, "
                "name text)"
            )

        reader = ADBCReader(conn, local_db=":memory:", source_dialect="postgres")
        cols = _get_table_cols(reader, ("dbo", "identity_probe"), dialect="postgres")

        by_name = {c.column_name: c for c in cols}
        assert by_name["id"].is_identity is True, (
            "genuine postgres identity column must be reported as is_identity=True"
        )
        assert by_name["name"].is_identity is False
    finally:
        with conn.cursor() as c:
            c.execute("drop table if exists dbo.identity_probe")
        conn.close()
