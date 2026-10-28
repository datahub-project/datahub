from typing import Any, Dict

import pytest
from looker_sdk.sdk.api40.models import DBConnection

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.looker.looker_config import (
    LookerConnectionDefinition,
    LookerDashboardSourceConfig,
    LookerViewNamingPattern,
)
from datahub.ingestion.source.looker.looker_selection import DASHBOARD_KIND


class TestLookerConnectionDefinition:
    def test_bigquery_uses_host_as_project_and_database_as_schema(self) -> None:
        definition = LookerConnectionDefinition.from_looker_connection(
            DBConnection(
                name="bq",
                dialect_name="bigquery_standard_sql",
                host="My-Project",
                database="Analytics",
            )
        )
        assert definition.platform == "bigquery"
        assert definition.default_db == "my-project"
        assert definition.default_schema == "analytics"

    @pytest.mark.parametrize(
        "dialect_name, platform",
        [("snowflake", "snowflake"), ("spark1", "spark"), ("druid_18", "druid")],
    )
    def test_generic_platform_is_derived_from_dialect(
        self, dialect_name: str, platform: str
    ) -> None:
        definition = LookerConnectionDefinition.from_looker_connection(
            DBConnection(
                name="c", dialect_name=dialect_name, database="DB", schema="Public"
            )
        )
        assert definition.platform == platform
        assert definition.default_db == "db"
        assert definition.default_schema == "public"

    def test_two_level_platform_has_no_schema(self) -> None:
        definition = LookerConnectionDefinition.from_looker_connection(
            DBConnection(name="c", dialect_name="mysql", database="db")
        )
        assert definition.default_schema is None

    def test_missing_dialect_is_a_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError):
            LookerConnectionDefinition.from_looker_connection(
                DBConnection(name="c", dialect_name=None)
            )

    def test_platform_env_is_validated(self) -> None:
        assert (
            LookerConnectionDefinition(
                platform="postgres", default_db="db", platform_env="DEV"
            ).platform_env
            == "DEV"
        )
        with pytest.raises(ValueError):
            LookerConnectionDefinition(
                platform="postgres", default_db="db", platform_env="nope"
            )


class TestNamingPattern:
    def test_rejects_unknown_variables(self) -> None:
        pattern = LookerViewNamingPattern(pattern="{project}.{bogus}")
        with pytest.raises(ValueError, match="bogus"):
            pattern.validate_pattern(at_least_one=True)

    def test_requires_at_least_one_variable(self) -> None:
        pattern = LookerViewNamingPattern(pattern="static")
        with pytest.raises(ValueError):
            pattern.validate_pattern(at_least_one=True)
        assert pattern.validate_pattern(at_least_one=False)

    def test_replace_variables_ignores_unset_values(self) -> None:
        pattern = LookerViewNamingPattern(pattern="{project}.{name}")
        assert (
            pattern.replace_variables({"project": "p", "name": "n", "model": None})
            == "p.n"
        )


class TestLookerDashboardSourceConfig:
    def _raw(self, **overrides: Any) -> Dict[str, Any]:
        raw: Dict[str, Any] = {
            "base_url": "https://looker.example.com:19999",
            "client_id": "id",
            "client_secret": "secret",
        }
        raw.update(overrides)
        return raw

    def test_external_base_url_defaults_to_base_url(self) -> None:
        config = LookerDashboardSourceConfig.model_validate(self._raw())
        assert config.external_base_url == "https://looker.example.com:19999"
        explicit = LookerDashboardSourceConfig.model_validate(
            self._raw(external_base_url="https://looker.example.com")
        )
        assert explicit.external_base_url == "https://looker.example.com"

    def test_independent_looks_require_stateful_ingestion(self) -> None:
        with pytest.raises(ValueError, match="stateful_ingestion"):
            LookerDashboardSourceConfig.model_validate(
                self._raw(extract_independent_looks=True)
            )
        config = LookerDashboardSourceConfig.model_validate(
            self._raw(
                extract_independent_looks=True, stateful_ingestion={"enabled": True}
            )
        )
        assert config.extract_independent_looks

    def test_probe_ancestor_kinds(self) -> None:
        config = LookerDashboardSourceConfig.model_validate(self._raw())
        assert config.probe_ancestor_kinds(DASHBOARD_KIND) == ()
        assert config.probe_ancestor_kinds("folder") is None
