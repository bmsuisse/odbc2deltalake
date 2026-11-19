"""A connection drop while make_writer reads the source metadata is retried via WriteConfig.retry_attempts."""

from typing import Any
from unittest.mock import patch

import pytest

from odbc2deltalake import WriteConfig, make_writer


def _flaky(fail_times: int) -> Any:
    calls: list[int] = []

    def _fn(*a: Any, **kw: Any) -> Any:
        calls.append(1)
        if len(calls) <= fail_times:
            raise ConnectionError("simulated connection drop")
        return "writer"

    return _fn, calls


def test_make_writer_retries(tmp_path: Any) -> None:
    fn, calls = _flaky(2)
    with patch("odbc2deltalake.write_init._make_writer", side_effect=fn):
        res = make_writer("bogus", "t", tmp_path, WriteConfig(retry_attempts=3, retry_wait_seconds=0))
    assert res == "writer"
    assert len(calls) == 3


def test_make_writer_no_retry_by_default(tmp_path: Any) -> None:
    fn, calls = _flaky(5)
    with patch("odbc2deltalake.write_init._make_writer", side_effect=fn), pytest.raises(ConnectionError):
        make_writer("bogus", "t", tmp_path, WriteConfig())
    assert len(calls) == 1
