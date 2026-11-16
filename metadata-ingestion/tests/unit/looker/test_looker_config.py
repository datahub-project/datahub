from typing import Callable

import pytest
from looker_sdk.sdk.api40.models import DBConnection
from pydantic import ValidationError

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.looker.looker_config import (
    LookerCommonConfig,
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

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "NamingPattern hooks validate_pattern in through __get_validators__, "
            "which pydantic v2 never calls, so a recipe with an unknown "
            "variable in view_naming_pattern is accepted and only fails later "
            "when the pattern is formatted"
        ),
    )
    def test_config_rejects_pattern_with_unknown_variable(self) -> None:
        with pytest.raises(ValidationError, match="bogus"):
            LookerCommonConfig.model_validate({"view_naming_pattern": "{bogus}"})

    def test_replace_variables_ignores_unset_values(self) -> None:
        pattern = LookerViewNamingPattern(pattern="{project}.{name}")
        assert (
            pattern.replace_variables({"project": "p", "name": "n", "model": None})
            == "p.n"
        )


ConfigFactory = Callable[..., LookerDashboardSourceConfig]


class TestLookerDashboardSourceConfig:
    def test_external_base_url_defaults_to_base_url(
        self, looker_config_factory: ConfigFactory
    ) -> None:
        internal = "https://looker.example.com:19999"
        config = looker_config_factory(base_url=internal)
        assert config.external_base_url == internal
        explicit = looker_config_factory(
            base_url=internal, external_base_url="https://looker.example.com"
        )
        assert explicit.external_base_url == "https://looker.example.com"

    def test_independent_looks_require_stateful_ingestion(
        self, looker_config_factory: ConfigFactory
    ) -> None:
        """Independent looks are soft-deleted through stateful ingestion, so
        enabling them without it is rejected at config time."""
        with pytest.raises(ValueError, match="stateful_ingestion"):
            looker_config_factory(extract_independent_looks=True)
        config = looker_config_factory(
            extract_independent_looks=True, stateful_ingestion={"enabled": True}
        )
        assert config.extract_independent_looks

    def test_probe_ancestor_kinds(self, looker_config_factory: ConfigFactory) -> None:
        config = looker_config_factory()
        assert config.probe_ancestor_kinds(DASHBOARD_KIND) == ()
        assert config.probe_ancestor_kinds("folder") is None
