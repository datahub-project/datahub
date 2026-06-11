import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.auth.k8s_projected import K8sProjectedTokenProvider


def test_reads_token_file_and_strips(tmp_path):
    f = tmp_path / "token"
    f.write_text("  jwt-token\n")
    provider = K8sProjectedTokenProvider.create({"token_file": str(f)})
    assert provider.get_token() == "jwt-token"


def test_rereads_on_each_call(tmp_path):
    f = tmp_path / "token"
    f.write_text("first")
    provider = K8sProjectedTokenProvider.create({"token_file": str(f)})
    assert provider.get_token() == "first"
    f.write_text("second")  # kubelet rotates the file
    assert provider.get_token() == "second"


def test_missing_file_raises_configuration_error(tmp_path):
    provider = K8sProjectedTokenProvider.create({"token_file": str(tmp_path / "nope")})
    with pytest.raises(ConfigurationError):
        provider.get_token()
