import pytest
from pydantic import ValidationError

from datahub.ingestion.source.informatica.config import InformaticaSourceConfig


def _base_config(**overrides):
    defaults = {"username": "svc@acme.com", "password": "hunter2"}
    defaults.update(overrides)
    return defaults


class TestInformaticaSourceConfig:
    def test_login_url_trailing_slash_is_stripped(self):
        config = InformaticaSourceConfig.model_validate(
            _base_config(login_url="https://dm-em.informaticacloud.com/")
        )
        assert config.login_url == "https://dm-em.informaticacloud.com"

    def test_login_url_rejects_non_https(self):
        with pytest.raises(ValidationError, match="login_url must start with https"):
            InformaticaSourceConfig.model_validate(
                _base_config(login_url="http://dm-us.informaticacloud.com")
            )

    def test_page_size_bounds(self):
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.model_validate(_base_config(page_size=0))
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.model_validate(_base_config(page_size=201))

    def test_export_batch_size_bounds(self):
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.model_validate(_base_config(export_batch_size=0))
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.model_validate(_base_config(export_batch_size=1001))

    def test_poll_interval_must_be_less_than_timeout(self):
        with pytest.raises(ValidationError, match="interval_secs must be less"):
            InformaticaSourceConfig.model_validate(
                _base_config(export_poll_timeout_secs=30, export_poll_interval_secs=30)
            )

    def test_connection_type_overrides_rejects_empty_platform(self):
        with pytest.raises(ValidationError, match="empty platform name"):
            InformaticaSourceConfig.model_validate(
                _base_config(connection_type_overrides={"01A": ""})
            )

    def test_connection_type_overrides_warns_on_unknown_platform(self, caplog):
        with caplog.at_level("WARNING"):
            InformaticaSourceConfig.model_validate(
                _base_config(connection_type_overrides={"01A": "snowflak_typo"})
            )
        assert any("snowflak_typo" in r.message for r in caplog.records)
