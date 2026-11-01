from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from duckdb import DuckDBPyConnection


def duckdb_arrow_reader(con: "DuckDBPyConnection", sql: str) -> "pa.RecordBatchReader":
    """Run `sql` on a DuckDB connection/cursor and return a streaming Arrow reader.

    `DuckDBPyConnection.fetch_record_batch` was removed in DuckDB 2.0, so this goes through
    a relation (`fetch_arrow_reader` exists in DuckDB 1.x and 2.x).
    """
    return con.sql(sql).fetch_arrow_reader()
