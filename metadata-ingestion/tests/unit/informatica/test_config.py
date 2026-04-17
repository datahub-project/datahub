import pytest
from pydantic import ValidationError

from datahub.ingestion.source.informatica.config import (
    DEFAULT_EXPORT_BATCH_SIZE,
    DEFAULT_PAGE_SIZE,
    InformaticaSourceConfig,
)


def _base_config(**overrides):
    defaults = {"username": "svc@acme.com", "password": "hunter2"}
    defaults.update(overrides)
    return defaults


class TestInformaticaSourceConfig:
    def test_minimal_config_accepts_defaults(self):
        config = InformaticaSourceConfig.parse_obj(_base_config())
        assert config.username == "svc@acme.com"
        assert config.password.get_secret_value() == "hunter2"
        assert config.login_url == "https://dm-us.informaticacloud.com"
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.export_batch_size == DEFAULT_EXPORT_BATCH_SIZE
        assert config.extract_lineage is True
        assert config.extract_ownership is True
        assert config.extract_tags is True

    def test_login_url_trailing_slash_is_stripped(self):
        config = InformaticaSourceConfig.parse_obj(
            _base_config(login_url="https://dm-em.informaticacloud.com/")
        )
        assert config.login_url == "https://dm-em.informaticacloud.com"

    def test_login_url_rejects_non_https(self):
        with pytest.raises(ValidationError, match="login_url must start with https"):
            InformaticaSourceConfig.parse_obj(
                _base_config(login_url="http://dm-us.informaticacloud.com")
            )

    def test_page_size_bounds(self):
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.parse_obj(_base_config(page_size=0))
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.parse_obj(_base_config(page_size=201))

    def test_export_batch_size_bounds(self):
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.parse_obj(_base_config(export_batch_size=0))
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.parse_obj(_base_config(export_batch_size=1001))

    def test_missing_username_or_password_rejected(self):
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.parse_obj({"password": "x"})
        with pytest.raises(ValidationError):
            InformaticaSourceConfig.parse_obj({"username": "x"})

    def test_tag_and_pattern_filters(self):
        config = InformaticaSourceConfig.parse_obj(
            _base_config(
                tag_filter_names=["pii", "critical"],
                project_pattern={"allow": ["Prod.*"], "deny": [".*_sandbox"]},
            )
        )
        assert config.tag_filter_names == ["pii", "critical"]
        assert config.project_pattern.allowed("Prod_sales") is True
        assert config.project_pattern.allowed("Prod_sales_sandbox") is False
        assert config.project_pattern.allowed("Dev_sales") is False

    def test_connection_type_overrides(self):
        config = InformaticaSourceConfig.parse_obj(
            _base_config(connection_type_overrides={"01DM180B": "snowflake"})
        )
        assert config.connection_type_overrides == {"01DM180B": "snowflake"}
