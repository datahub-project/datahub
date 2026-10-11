import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.auth.pat import PatTokenProvider


def test_static_token():
    provider = PatTokenProvider.create({"token": "pat-value"})
    result = provider.get_token()
    assert result.token == "pat-value"
    assert result.expires_at is None


def test_reads_token_file_and_strips(tmp_path):
    f = tmp_path / "token"
    f.write_text("  pat-from-file\n")
    provider = PatTokenProvider.create({"token_file": str(f)})
    assert provider.get_token().token == "pat-from-file"


def test_rereads_file_on_each_call(tmp_path):
    f = tmp_path / "token"
    f.write_text("first")
    provider = PatTokenProvider.create({"token_file": str(f)})
    assert provider.get_token().token == "first"
    f.write_text("second")  # rotated mounted Secret
    assert provider.get_token().token == "second"


def test_missing_file_raises_configuration_error(tmp_path):
    provider = PatTokenProvider.create({"token_file": str(tmp_path / "nope")})
    with pytest.raises(ConfigurationError, match="nope"):
        provider.get_token()


def test_requires_exactly_one_source(tmp_path):
    with pytest.raises(ConfigurationError):
        PatTokenProvider.create({})
    with pytest.raises(ConfigurationError):
        PatTokenProvider.create(
            {"token": "pat-value", "token_file": str(tmp_path / "token")}
        )


def test_token_masked_in_config_repr():
    config_repr = repr(PatTokenProvider.create({"token": "pat-value"})._token)
    assert "pat-value" not in config_repr
