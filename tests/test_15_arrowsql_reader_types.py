import datetime as dt
import pyarrow as pa
import pytest

from odbc2deltalake.reader.arrowsql_reader import (
    _normalize_batch,
    _normalize_schema,
    strip_odbc_driver,
)


@pytest.mark.parametrize(
    "conn_str,expected",
    [
        (
            "DRIVER=ODBC Driver 18 for SQL Server;Server=a;Database=b",
            "Server=a;Database=b",
        ),
        ("Server=a;Driver={ODBC Driver 18 for SQL Server};UID=x", "Server=a;UID=x"),
        ("Server=a;Database=b", "Server=a;Database=b"),
    ],
)
def test_strip_odbc_driver(conn_str: str, expected: str):
    assert strip_odbc_driver(conn_str) == expected


def test_arrowsql_types_match_odbc_reader_types():
    schema = pa.schema(
        [
            ("s", pa.large_string()),
            ("b", pa.large_binary()),
            ("tinyint", pa.uint8()),
            ("t", pa.time64("ns")),
            ("dto", pa.timestamp("us", tz="+00:00")),
            ("ts", pa.timestamp("us")),
            ("d", pa.decimal128(18, 2)),
        ]
    )
    target = _normalize_schema(schema)
    assert target.types == [
        pa.string(),
        pa.binary(),
        pa.int16(),
        pa.string(),
        pa.timestamp("us", tz="UTC"),
        pa.timestamp("us"),
        pa.decimal128(18, 2),
    ]
    batch = pa.RecordBatch.from_pydict(
        {
            "s": ["a", None],
            "b": [b"\x01", None],
            "tinyint": [255, None],
            "t": [dt.time(10, 0, 1, 123000), None],
            "dto": [dt.datetime(2020, 1, 1, 9), None],
            "ts": [dt.datetime(2020, 1, 1), None],
            "d": [None, None],
        },
        schema=schema,
    )
    out = _normalize_batch(batch, target)
    assert out.schema.equals(target)
    assert out.column("t").to_pylist() == ["10:00:01.1230000", None]
    assert out.column("tinyint").to_pylist() == [255, None]
