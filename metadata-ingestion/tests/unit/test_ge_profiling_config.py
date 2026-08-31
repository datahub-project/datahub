import pytest

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.source.ge_profiling_config import (
    GEProfilingConfig,
    ProfilingIsolationLevel,
)


def test_profile_table_level_only():
    config = GEProfilingConfig.model_validate(
        {"enabled": True, "profile_table_level_only": True}
    )
    assert config.any_field_level_metrics_enabled() is False

    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "profile_table_level_only": True,
            "include_field_max_value": False,
        }
    )
    assert config.any_field_level_metrics_enabled() is False


def test_profile_table_level_only_fails_with_field_metric_enabled():
    with pytest.raises(
        ValueError,
        match="Cannot enable field-level metrics if profile_table_level_only is set",
    ):
        GEProfilingConfig.model_validate(
            {
                "enabled": True,
                "profile_table_level_only": True,
                "include_field_max_value": True,
            }
        )


def test_profiling_method_field_removed() -> None:
    # `method` was removed together with the Great Expectations profiler.
    # SQLAlchemy is the only SQL profiler; recipes that still set `method` are
    # accepted (the field is dropped) with a deprecation warning.
    with pytest.warns(ConfigurationWarning, match="method was removed"):
        config = GEProfilingConfig.model_validate({"enabled": True, "method": "ge"})
    assert not hasattr(config, "method")


def test_profiling_isolation_level_default_is_none():
    # Unset by default: nothing is set on the profiling connection, so one
    # transaction spans the whole table profile.
    config = GEProfilingConfig.model_validate({"enabled": True})
    assert config.profiling_isolation_level is None


def test_profiling_isolation_level_normalizes_case_and_underscores():
    # The validator accepts case/underscore variants so the enum rejects typos
    # at config-parse time while still matching the SQL standard names with
    # spaces.
    assert (
        GEProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "autocommit"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.AUTOCOMMIT
    )
    assert (
        GEProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "read_committed"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.READ_COMMITTED
    )
    assert (
        GEProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "read committed"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.READ_COMMITTED
    )
    assert (
        GEProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "REPEATABLE_READ"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.REPEATABLE_READ
    )


def test_profiling_isolation_level_empty_string_is_none():
    # Empty/whitespace normalizes to None, i.e. leave the connection alone.
    assert (
        GEProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "  "}
        ).profiling_isolation_level
        is None
    )


def test_profiling_isolation_level_rejects_unknown_value():
    # The enum prevents typos at config-parse time.
    with pytest.raises(ValueError):
        GEProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "BOGUS"}
        )


def test_profiling_isolation_level_json_schema_has_default():
    # docs_config_table.py gates the "Default:" line on `"default" in json_props`.
    # Field(default=None) emits that key; default_factory does not.
    schema = GEProfilingConfig.model_json_schema()
    assert "default" in schema["properties"]["profiling_isolation_level"]
