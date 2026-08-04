"""_get_default_token cached a single DefaultAzureCredential in a
process-wide global keyed only on "the first credential ever built",
ignoring the actual kwargs on every subsequent call. A pipeline reading
from one storage account (managed_identity_client_id=A) and writing to
another (managed_identity_client_id=B) would silently authenticate the
second destination with the first destination's identity.
"""
from unittest.mock import MagicMock, patch

import odbc2deltalake.destination.azure_utils as azure_utils


def test_different_kwargs_get_different_cached_credentials():
    azure_utils._token_state.clear()

    created_with: list[dict] = []

    class FakeCredential:
        def __init__(self, **kwargs):
            created_with.append(kwargs)
            self.kwargs = kwargs

        def get_token(self, *_a, **_kw):
            return MagicMock(token=f"token-for-{self.kwargs}")

    with patch("azure.identity.DefaultAzureCredential", FakeCredential):
        token_a = azure_utils._get_default_token(managed_identity_client_id="A")
        token_b = azure_utils._get_default_token(managed_identity_client_id="B")
        # same kwargs again -> should reuse the cached credential, not build a 3rd
        token_a_again = azure_utils._get_default_token(managed_identity_client_id="A")

    assert token_a != token_b, (
        "different managed_identity_client_id kwargs must not silently reuse "
        "the first credential's token"
    )
    assert token_a == token_a_again
    assert len(created_with) == 2, (
        f"expected exactly 2 DefaultAzureCredential instances (one per distinct "
        f"kwargs), got {len(created_with)}: {created_with}"
    )
