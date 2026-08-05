import pytest

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig


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
