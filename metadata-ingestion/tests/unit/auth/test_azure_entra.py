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
            return types.SimpleNamespace(token=f"tok-for-{scope}")

    fake.ClientSecretCredential = _Cred  # type: ignore[attr-defined]
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
    assert provider.get_token() == "tok-for-api://x/.default"


def test_missing_extra_raises(monkeypatch):
    monkeypatch.setitem(sys.modules, "azure.identity", None)
    with pytest.raises(Exception) as ei:
        AzureEntraTokenProvider.create(
            {"tenant_id": "t", "client_id": "c", "scope": "s"}
        )
    assert "azure-auth" in str(ei.value)
