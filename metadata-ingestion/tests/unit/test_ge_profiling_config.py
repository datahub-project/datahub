import pytest

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig


def test_profile_table_level_only():
    config = GEProfilingConfig.parse_obj(
        {"enabled": True, "profile_table_level_only": True}
    )
    assert config.any_field_level_metrics_enabled() is False

    config = GEProfilingConfig.parse_obj(
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
        GEProfilingConfig.parse_obj(
            {
                "enabled": True,
                "profile_table_level_only": True,
                "include_field_max_value": True,
            }
        )
