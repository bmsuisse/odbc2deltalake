from typing import Callable, Literal, Optional, Union


default_azure_args = [
    "authority",
    "exclude_workload_identity_credential",
    "exclude_developer_cli_credential",
    "exclude_cli_credential",
    "exclude_environment_credential",
    "exclude_managed_identity_credential",
    "exclude_powershell_credential",
    "exclude_visual_studio_code_credential",
    "exclude_shared_token_cache_credential",
    "exclude_interactive_browser_credential",
    "interactive_browser_tenant_id",
    "managed_identity_client_id",
    "workload_identity_client_id",
    "workload_identity_tenant_id",
    "interactive_browser_client_id",
    "shared_cache_username",
    "shared_cache_tenant_id",
    "visual_studio_code_tenant_id",
    "process_timeout",
]


_token_state = dict()


def _get_default_token(**kwargs) -> str:
    global _token_state
    from azure.identity import DefaultAzureCredential

    cred: Union[DefaultAzureCredential, None] = _token_state.get("cred", None)
    if not cred:
        cred = DefaultAzureCredential(**kwargs)
        _token_state["cred"] = cred
    return cred.get_token("https://storage.azure.com/.default").token


def convert_options(
    options: Union[dict, None],
    flavor: Literal["fsspec", "object_store"],
    token_retrieval_func: Optional[Callable[[], str]] = None,
):
    if options is None:
        return None
    use_emulator: bool = options.get("use_emulator", "0").lower() in ["1", "true"]
    if flavor == "fsspec" and "connection_string" not in options and use_emulator:
        constr = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
        return {"connection_string": constr}
    elif (
        flavor == "fsspec"
        and "account_key" not in options
        and "anon" not in options
        and "sas_token" not in options
        and "token" not in options
        and "account_name" in options
    ):  # anon is true by default in fsspec which makes no sense mostly
        return {"anon": False} | options

    new_opts = options.copy()
    anon_value = False
    if flavor == "object_store" and "anon" in options:
        anon_value = new_opts.pop("anon")
    if (
        flavor == "object_store"
        and "account_key" not in options
        and "sas_token" not in options
        and "token" not in options
        and not use_emulator
        and not anon_value
    ):
        token_kwargs = {k: new_opts.pop(k) for k in default_azure_args if k in new_opts}
        new_opts["token"] = (token_retrieval_func or _get_default_token)(**token_kwargs)
    return new_opts
