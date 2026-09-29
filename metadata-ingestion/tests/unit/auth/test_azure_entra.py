import sys
import types

import pytest

from datahub.ingestion.auth.azure_entra import AzureEntraTokenProvider


def _install_fake_azure_identity(monkeypatch):
    fake = types.ModuleType("azure.identity")

    class _Cred:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def get_token(self, scope):
            # Mirrors azure.core.credentials.AccessToken(token, expires_on).
            return types.SimpleNamespace(
                token=f"tok-for-{scope}", expires_on=1893456000
            )

    class _SecretCred(_Cred):
        # Distinct token so a test can tell which credential class was selected.
        def get_token(self, scope):
            return types.SimpleNamespace(
                token=f"secret-tok-for-{scope}", expires_on=1893456000
            )

    fake.ClientSecretCredential = _SecretCred  # type: ignore[attr-defined]
    fake.WorkloadIdentityCredential = _Cred  # type: ignore[attr-defined]
    azure_pkg = types.ModuleType("azure")
    azure_pkg.identity = fake  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure", azure_pkg)
    monkeypatch.setitem(sys.modules, "azure.identity", fake)
    return _Cred


def test_workload_identity_when_no_secret(monkeypatch):
    _install_fake_azure_identity(monkeypatch)
    provider = AzureEntraTokenProvider.create(
        {"tenant_id": "t", "client_id": "c", "scope": "api://x/.default"}
    )
    result = provider.get_token()
    assert result.token == "tok-for-api://x/.default"
    # expires_at comes straight from azure-identity's expires_on.
    assert result.expires_at == 1893456000.0


def test_client_secret_selects_client_secret_credential(monkeypatch):
    _install_fake_azure_identity(monkeypatch)
    provider = AzureEntraTokenProvider.create(
        {
            "tenant_id": "t",
            "client_id": "c",
            "scope": "api://x/.default",
            "client_secret": "shh",
        }
    )
    result = provider.get_token()
    # A client_secret selects ClientSecretCredential (interim path) over
    # workload identity — distinguished here by the returned token.
    assert result.token == "secret-tok-for-api://x/.default"
    assert result.expires_at == 1893456000.0


def test_missing_extra_raises(monkeypatch):
    monkeypatch.setitem(sys.modules, "azure.identity", None)
    with pytest.raises(Exception) as ei:
        AzureEntraTokenProvider.create(
            {"tenant_id": "t", "client_id": "c", "scope": "s"}
        )
    assert "azure-auth" in str(ei.value)
