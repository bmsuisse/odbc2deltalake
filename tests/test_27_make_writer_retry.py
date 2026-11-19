"""A connection drop while make_writer reads the source metadata is retried via WriteConfig.retry_attempts."""

from typing import Any
from unittest.mock import patch

import pytest

from odbc2deltalake import WriteConfig, make_writer
from odbc2deltalake.write_init import _make_writer


def _flaky(fail_times: int) -> Any:
    calls: list[int] = []

    def _wrapper(*a: Any, **kw: Any) -> Any:
        calls.append(1)
        if len(calls) <= fail_times:
            raise ConnectionError("simulated connection drop")
        return _make_writer(*a, **kw)

    return _wrapper, calls


def test_make_writer_retries(tmp_path: Any) -> None:
    wrapper, calls = _flaky(2)
    cfg = WriteConfig(retry_attempts=3, retry_wait_seconds=0)
    with patch("odbc2deltalake.write_init._make_writer", side_effect=lambda *a, **k: _raise_or(wrapper, a, k)):
        with pytest.raises(Exception):  # attempt 3 reaches the real function, which fails on the bogus source
            make_writer("bogus", "t", tmp_path, cfg)
    assert len(calls) == 3


def _raise_or(wrapper: Any, a: Any, k: Any) -> Any:
    return wrapper(*a, **k)


def test_make_writer_no_retry_by_default(tmp_path: Any) -> None:
    wrapper, calls = _flaky(5)
    with patch("odbc2deltalake.write_init._make_writer", side_effect=lambda *a, **k: _raise_or(wrapper, a, k)):
        with pytest.raises(ConnectionError):
            make_writer("bogus", "t", tmp_path, WriteConfig())
    assert len(calls) == 1
