# AGENTS.md

Notes for coding agents working in this repo. Written from the pain points hit
during actual setup, not from reading the code.

## What this is

`odbc2deltalake`: reads from ODBC/ADBC sources (MS SQL Server, Postgres) and
writes SCD2-style Delta tables. Core entry point: `write_db_to_delta` in
`odbc2deltalake/__init__.py`. Source of truth for behavior is
`odbc2deltalake/write_init.py`, `db_to_delta.py`, `consistency.py`,
`write_utils/restore_pk.py`.

## Environment setup

```bash
uv sync --extra local --group test --extra postgres   # fast local loop (postgres source, no spark/mssql odbc driver needed)
```

For the full CI-equivalent matrix (see `.github/workflows/python-test.yml`):

```bash
uv sync --extra local --extra local_azure --group test --group dev --extra postgres   # or --extra mssql
```

`--extra mssql` requires a real ODBC driver (`msodbcsql18`) installed on the
host — `pyodbc` alone is not enough. Postgres via `adbc-driver-postgresql`
needs no system driver, so it's the faster path for local iteration.

## Test containers: podman, not docker

The repo's `test_server/__init__.py` uses the `docker` Python package via
`docker.from_env()`. On a machine with only **podman** installed (no Docker
Desktop), this fails silently/hangs unless `DOCKER_HOST` points at the podman
socket:

```bash
podman machine inspect podman-machine-default | grep -A2 PodmanSocket
export DOCKER_HOST=unix:///path/from/above/podman-machine-default-api.sock
```

Containers are named `test4{server}_odbc2deltalake` (mssql/postgres) and
`test4azurite`, and are reused across runs if already present — a stale/broken
container silently short-circuits the fixture's "already running" check. If
tests hang or connect to a wrong DB state, check `podman ps -a` and remove the
container rather than assuming it's fresh.

`mcr.microsoft.com/mssql/server:2022-latest` has no native arm64 image —
expect emulation weirdness/slowness on Apple Silicon. Postgres 17.5 has one.

## Test config knobs (env vars, see `tests/conftest.py`)

- `ODBCLAKE_TEST_SOURCE_SERVER`: `mssql` (default) or `postgres`
- `ODBCLAKE_TEST_CONFIGURATION`: `local`, `spark`, or `azure` (default runs
  all three per test if unset — slow). Set to `local` for a fast loop.
- `NO_SQL_SERVER=1`: skip spawning the DB container (bring your own)
- `NO_AZURITE_DOCKER=1`: skip azurite container (only matters if config is
  `azure`)
- `NO_SPARK=1`: skip the spark session fixture entirely
- `KEEP_SQL_SERVER=1` / `KEEP_AZURITE_DOCKER=1`: don't stop containers after
  the session — reuse them on the next run instead of paying container
  startup cost every time.

Fastest local dev loop:

```bash
export DOCKER_HOST=unix:///...podman-machine-default-api.sock
export ODBCLAKE_TEST_SOURCE_SERVER=postgres
export ODBCLAKE_TEST_CONFIGURATION=local
export NO_SPARK=1
export NO_AZURITE_DOCKER=1
uv run pytest tests/ -q
```

## Misc

- macOS has no `timeout`/`gtimeout` by default (no coreutils). Don't script
  around it — just run commands with the harness's own timeout/background
  mechanism.
- `pyproject.toml`'s `requires-python = "~=3.9"` triggers a uv warning on
  every command (tilde-without-patch ambiguity). Harmless, not worth fixing
  unless touching packaging config anyway.
- Tests are numbered/ordered (`test_01_...` through `test_12_...`,
  `pytest-order`) — later tests build on state from earlier ones within a
  file/session. Don't run individual tests out of order expecting isolation.
