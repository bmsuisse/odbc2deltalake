from pathlib import Path
import docker
from docker.models.containers import Container
from time import sleep
from typing import Union, cast, Literal
import docker.errors
import os

server_configs = {
    "mssql": {
        "container_port": 1443,
        "local_port": 1444,
        "image": "mcr.microsoft.com/mssql/server:2022-latest",
    },
    "postgres": {"container_port": 5432, "local_port": 54320, "image": "postgres:17.5"},
}


def _getenvs(server: Literal["mssql", "postgres"]):
    envs = dict()
    with open(f"test_server/{server}.env", "r") as f:
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


def start_server(server: Literal["mssql", "postgres"]) -> Container:
    client = (
        docker.from_env()
    )  # code taken from https://github.com/fsspec/adlfs/blob/main/adlfs/tests/conftest.py#L72
    sql_server: Union[Container, None] = None
    try:
        m = cast(Container, client.containers.get(f"test4{server}_odbc2deltalake"))
        if m.status == "running":
            return m
        else:
            sql_server = m
    except docker.errors.NotFound:
        pass

    envs = _getenvs(server)

    if sql_server is None:
        # using podman:  podman run  --env-file=TESTS/SQL_DOCKER.ENV --publish=1439:1433 --name=mssql1 chriseaton/adventureworks:light
        #                podman kill mssql1
        sql_server = client.containers.run(
            server_configs[server]["image"],
            environment=envs,
            detach=True,
            name=f"test4{server}_odbc2deltalake",
            ports={
                f"{server_configs[server]['container_port']}/tcp": server_configs[
                    server
                ]["local_port"]
            },
        )  # type: ignore
    assert sql_server is not None
    sql_server.start()
    print(sql_server.status)
    sleep(20)
    print(f"Successfully created {server} container...")
    return sql_server


def create_test_blobstorage():
    constr = os.getenv(
        "TEST_BLOB_CONSTR",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
    )
    from azure.storage.blob import ContainerClient

    cc = ContainerClient.from_connection_string(constr, "testlakeodbc")
    if not cc.exists():
        cc.create_container()
    else:
        cc.delete_container()
        cc.create_container()
    return cc


def start_azurite() -> Container:
    client = (
        docker.from_env()
    )  # code taken from https://github.com/fsspec/adlfs/blob/main/adlfs/tests/conftest.py#L72
    azurite_server: Union[Container, None] = None
    try:
        m = cast(Container, client.containers.get("test4azurite"))
        if m.status == "running":
            create_test_blobstorage()
            return m
        else:
            azurite_server = m
    except docker.errors.NotFound:
        pass

    if azurite_server is None:
        azurite_server = client.containers.run(
            "mcr.microsoft.com/azure-storage/azurite:latest",
            detach=True,
            name="test4azurite",
            ports={"10000/tcp": 10000, "10001/tcp": 10001, "10002/tcp": 10002},
        )  # type: ignore
    assert azurite_server is not None
    azurite_server.start()
    print(azurite_server.status)
    sleep(20)
    create_test_blobstorage()
    print("Successfully created azurite container...")
    return azurite_server
