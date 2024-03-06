from pathlib import Path
import docker
from docker.models.containers import Container
from time import sleep
from typing import cast
import docker.errors
import os


def _getenvs():
    envs = dict()
    with open("test_server/sql_docker.env", "r") as f:
        lines = f.readlines()
        envs = {
            item[0].strip(): item[1].strip()
            for item in [
                line.split("=")
                for line in lines
                if len(line.strip()) > 0 and not line.startswith("#")
            ]
        }
    return envs


def start_mssql_server() -> Container:
    client = (
        docker.from_env()
    )  # code taken from https://github.com/fsspec/adlfs/blob/main/adlfs/tests/conftest.py#L72
    sql_server: Container | None = None
    try:
        m = cast(Container, client.containers.get("test4sql_odbc2deltalake"))
        if m.status == "running":
            return m
        else:
            sql_server = m
    except docker.errors.NotFound:
        pass

    envs = _getenvs()

    if sql_server is None:
        # using podman:  podman run  --env-file=TESTS/SQL_DOCKER.ENV --publish=1439:1433 --name=mssql1 chriseaton/adventureworks:light
        #                podman kill mssql1
        sql_server = client.containers.run(
            "mcr.microsoft.com/mssql/server:2022-latest",
            environment=envs,
            detach=True,
            name="test4sql_odbc2deltalake",
            ports={"1433/tcp": "1444"},
        )  # type: ignore
    assert sql_server is not None
    sql_server.start()
    print(sql_server.status)
    sleep(15)
    print("Successfully created sql container...")
    return sql_server
