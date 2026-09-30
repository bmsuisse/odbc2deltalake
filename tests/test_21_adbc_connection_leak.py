"""ADBCReader (the Postgres/ADBC source reader) never called .commit() on its
underlying connection anywhere - not in source_sql_to_py,
source_schema_limit_one, or source_write_sql_to_delta. Since ADBC/postgres
connections are not autocommit by default, every read left an open,
uncommitted transaction holding locks on whatever table it touched, for the
entire remaining lifetime of the connection.

This is not a theoretical concern: it deadlocked the real test suite. Any
later DDL against a table the reader had ever read from (eg a test doing
`ALTER TABLE ... RENAME` to simulate a schema drift/restore scenario, or a
real production schema migration) blocks forever on that stale lock.
"""

from typing import TYPE_CHECKING
import pytest

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(22)
def test_adbc_reader_does_not_leak_locks_across_calls(connection: "DB_Connection"):
    if connection.source_server != "postgres":
        pytest.skip("this bug is specific to the ADBC/postgres reader")

    import adbc_driver_postgresql.dbapi as adbc_pg
    from odbc2deltalake.reader.adbc_reader import ADBCReader

    connstr = connection.conn_str["local"]
    reader_conn = adbc_pg.connect(connstr)
    reader = ADBCReader(reader_conn, local_db=":memory:", source_dialect="postgres")

    # any read through the reader (schema query, row fetch) must not leave
    # a lock-holding transaction open on the table it touched
    reader.source_sql_to_py("select id, name from dbo.company3 limit 1")

    check_conn = adbc_pg.connect(connstr, autocommit=True)
    try:
        with check_conn.cursor() as c:
            c.execute("set lock_timeout = '3s'")
            try:
                c.execute("alter table dbo.company3 add column zzz_leak_test int")
                c.execute("alter table dbo.company3 drop column zzz_leak_test")
            except Exception as e:
                pytest.fail(
                    "ALTER TABLE on a table the ADBCReader had read from was "
                    f"blocked - the reader is leaking a lock-holding, "
                    f"uncommitted transaction: {e}"
                )
    finally:
        check_conn.close()
        reader_conn.close()
